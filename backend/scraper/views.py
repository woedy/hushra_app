from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.views import APIView
from django.http import StreamingHttpResponse
from django.utils import timezone
from datetime import timedelta
import csv

from .models import HushraCredentials, SearchJob, SearchTask, PersonRecord, Proxy, GlobalSetting, StateRun
from .serializers import (
    HushraCredentialsSerializer, SearchJobSerializer, SearchTaskSerializer,
    PersonRecordSerializer, ProxySerializer, GlobalSettingSerializer, StateRunSerializer
)
from .tasks import execute_ssn_lookup, orchestrate_spider
from .hushra_client import HushraAPIClient
import django_filters.rest_framework
from django.core.paginator import Paginator


# ---------------------------------------------------------------------------

class GlobalSettingViewSet(viewsets.ModelViewSet):
    queryset = GlobalSetting.objects.all()
    serializer_class = GlobalSettingSerializer

    @action(detail=False, methods=['post'])
    def toggle(self, request):
        key = request.data.get('key')
        if not key:
            return Response({"error": "Key required"}, status=400)
        obj, _ = GlobalSetting.objects.get_or_create(key=key)
        # Flip string booleans
        current_bool = str(obj.value).lower() == 'true'
        obj.value = str(not current_bool).lower()
        obj.save()
        return Response(GlobalSettingSerializer(obj).data)

    @action(detail=False, methods=['post'])
    def set_value(self, request):
        key = request.data.get('key')
        value = request.data.get('value')
        if not key:
            return Response({"error": "Key required"}, status=400)
        obj, _ = GlobalSetting.objects.get_or_create(key=key)
        if isinstance(value, bool):
            normalized = str(value).lower()
        else:
            normalized = str(value)
        obj.value = normalized
        obj.save()
        return Response(GlobalSettingSerializer(obj).data)

    @action(detail=False, methods=['get'])
    def orchestrator_status(self, request):
        """Returns a rich status payload for the Auto Orchestrator panel."""
        enabled = GlobalSetting.get_value('auto_run_enabled', default='false')
        pending = SearchTask.objects.filter(status='PENDING').count()
        in_progress = SearchTask.objects.filter(status='IN_PROGRESS').count()
        completed = SearchTask.objects.filter(status='COMPLETED').count()
        failed = SearchTask.objects.filter(status='FAILED').count()
        min_queue = GlobalSetting.get_value('auto_queue_min', default='200')
        states_str = GlobalSetting.get_value('auto_run_states', default='')
        axes_str = GlobalSetting.get_value('auto_run_axes', default='lastname')
        states = [s.strip() for s in states_str.split(',') if s.strip()] if states_str else []
        axes = [a.strip() for a in axes_str.split(',') if a.strip()] if axes_str else ['lastname']

        # Calculate how many A-Z prime slots exist in total for configured states
        total_prime_slots = len(states) * len(axes) * 26
        total_credentials = HushraCredentials.objects.count()
        HushraCredentials.restore_ready_credentials()
        active_credentials = HushraCredentials.objects.filter(is_active=True).count()
        usable_credentials = HushraCredentials.objects.filter(is_active=True).count()

        return Response({
            'enabled': str(enabled).lower() == 'true',
            'pending': pending,
            'in_progress': in_progress,
            'completed': completed,
            'failed': failed,
            'total_active': pending + in_progress,
            'min_queue': int(min_queue),
            'states': states,
            'axes': axes,
            'total_prime_slots': total_prime_slots,
            'total_credentials': total_credentials,
            'active_credentials': active_credentials,
            'usable_credentials': usable_credentials,
            'credentials_ready': usable_credentials > 0,
        })

    @action(detail=False, methods=['post'])
    def seed_now(self, request):
        """Trigger an immediate orchestrator run regardless of the enabled flag."""
        states_str = GlobalSetting.get_value('auto_run_states', default='')
        if not states_str:
            return Response({'error': 'No states configured. Save settings first.'}, status=400)
        try:
            states = [s.strip() for s in states_str.split(',') if s.strip()]
            axes = [a.strip() for a in axes_str.split(',') if a.strip()] if (axes_str := GlobalSetting.get_value('auto_run_axes', default='lastname')) else []
        except Exception:
            return Response({'error': 'Invalid states/axes configuration.'}, status=400)
        if not states:
            return Response({'error': 'States cannot be empty.'}, status=400)

        if HushraCredentials.objects.count() == 0:
            return Response({'error': 'No UUID credentials found. Add UUIDs before starting Auto Run.'}, status=400)

        soft_limit = GlobalSetting.get_value('soft_limit', 100)
        if not HushraCredentials.has_usable_credentials(soft_limit=soft_limit):
            return Response({'error': 'No active UUID credentials are available right now. Reset the UUID pool or add new UUIDs.'}, status=400)

        # Run one orchestration cycle inline so the UI immediately shows seeded jobs/tasks,
        # then rely on worker execution for background lookup processing.
        result = orchestrate_spider(seed_anyway=True)
        return Response({'message': f'Seeding triggered successfully. {result}'})

    @action(detail=False, methods=['post'])
    def new_session(self, request):
        """
        Hard reset auto-run execution so old tasks can't continue unexpectedly.
        - disables auto_run
        - revokes pending/in-progress tasks
        - pauses active state runs
        - purges broker queue and clears terminal rows
        """
        from core.celery import app as celery_app

        GlobalSetting.objects.update_or_create(key='auto_run_enabled', defaults={'value': 'false'})

        try:
            job = SearchJob.objects.get(name="Auto Orchestrator Job")
        except SearchJob.DoesNotExist:
            return Response({'message': 'No existing auto-orchestrator job. Auto Run disabled.', 'deleted': 0, 'cancelled': 0})

        active_qs = job.tasks.filter(status__in=['PENDING', 'IN_PROGRESS'])
        to_revoke_ids = list(active_qs.exclude(celery_task_id__isnull=True).values_list('celery_task_id', flat=True))
        cancelled = active_qs.update(status='STOPPED', updated_at=timezone.now())

        revoked = 0
        for tid in to_revoke_ids:
            if tid and tid != 'PENDING_COMMIT':
                celery_app.control.revoke(tid, terminate=True)
                revoked += 1

        job.state_runs.filter(status='RUNNING').update(status='PAUSED', updated_at=timezone.now())
        job.status = 'RUNNING'
        job.save(update_fields=['status'])

        deleted_count, _ = job.tasks.filter(
            status__in=['COMPLETED', 'FAILED', 'STOPPED', 'ABORTED', 'TOO_BROAD']
        ).delete()

        with celery_app.connection_or_acquire() as conn:
            conn.default_channel.queue_purge('celery')

        return Response({
            'message': (
                f'New session ready. Auto Run disabled, cancelled {cancelled} active tasks '
                f'({revoked} revoked), and cleared {deleted_count} terminal tasks.'
            ),
            'deleted': deleted_count,
            'cancelled': cancelled,
            'revoked': revoked,
        })

    @action(detail=False, methods=['post'])
    def smoke_test(self, request):
        """
        Minimal manual pipeline: seed a small deterministic batch (default 5)
        for one state/axis and enqueue workers immediately.
        Payload (optional): {"state": "CA", "axis": "lastname", "count": 5, "clear_existing": true}
        """
        axis = str(request.data.get('axis', 'lastname')).strip().lower()
        if axis not in {'lastname', 'firstname', 'city'}:
            return Response({'error': 'axis must be one of lastname, firstname, city'}, status=400)

        raw_count = request.data.get('count', 5)
        try:
            count = max(1, min(int(raw_count), 26))
        except (TypeError, ValueError):
            return Response({'error': 'count must be an integer between 1 and 26'}, status=400)

        clear_existing = str(request.data.get('clear_existing', 'true')).lower() != 'false'

        state = str(request.data.get('state', '')).strip().upper()
        if not state:
            configured_states = GlobalSetting.get_value('auto_run_states', default='')
            if configured_states:
                parts = [s.strip().upper() for s in str(configured_states).split(',') if s.strip()]
                state = parts[0] if parts else ''
        if not state or len(state) != 2:
            return Response({'error': 'Provide a valid 2-letter state code.'}, status=400)

        if HushraCredentials.objects.count() == 0:
            return Response({'error': 'No UUID credentials found. Add UUIDs first.'}, status=400)

        HushraCredentials.restore_ready_credentials()
        if not HushraCredentials.objects.filter(is_active=True).exists():
            return Response({'error': 'No active UUID credentials are available. Reset UUID pool first.'}, status=400)

        alphabet = 'abcdefghijklmnopqrstuvwxyz'
        letters = alphabet[:count]

        job, _ = SearchJob.objects.get_or_create(name='Smoke Test Job', defaults={'status': 'RUNNING'})
        if job.status == 'STOPPED':
            job.status = 'RUNNING'
            job.save(update_fields=['status'])

        state_run, _ = StateRun.objects.get_or_create(
            job=job,
            state=state,
            defaults={'status': 'RUNNING', 'axes_enabled': [axis]},
        )
        state_run.status = 'RUNNING'
        state_run.axes_enabled = [axis]
        state_run.started_at = state_run.started_at or timezone.now()
        state_run.save(update_fields=['status', 'axes_enabled', 'started_at', 'updated_at'])

        deleted = 0
        if clear_existing:
            deleted, _ = state_run.tasks.filter(status__in=['PENDING', 'IN_PROGRESS']).delete()

        seeded_ids = []
        for letter in letters:
            task_kwargs = {
                'job': job,
                'state_run': state_run,
                'axis': axis,
                'state': state,
                'status': 'PENDING',
                'firstname': '',
                'lastname': '',
                'city': '',
            }
            if axis == 'lastname':
                task_kwargs['lastname'] = letter
            elif axis == 'firstname':
                task_kwargs['firstname'] = letter
            else:
                task_kwargs['city'] = letter

            task = SearchTask.objects.create(**task_kwargs)
            res = execute_ssn_lookup.delay(task.id)
            task.celery_task_id = res.id
            task.save(update_fields=['celery_task_id'])
            seeded_ids.append(task.id)

        state_run.update_metrics()

        return Response({
            'message': f'Smoke test seeded {len(seeded_ids)} tasks for {state}/{axis}.',
            'job_id': job.id,
            'state_run_id': state_run.id,
            'state': state,
            'axis': axis,
            'seeded_task_ids': seeded_ids,
            'cleared_active_tasks': deleted,
        })

    @action(detail=False, methods=['get'])
    def worker_health(self, request):
        """Basic runtime health snapshot for troubleshooting queue execution."""
        HushraCredentials.restore_ready_credentials()

        status_counts = {
            key: SearchTask.objects.filter(status=key).count()
            for key in ['PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'STOPPED', 'ABORTED', 'TOO_BROAD']
        }

        latest_tasks = list(
            SearchTask.objects.order_by('-updated_at')
            .values('id', 'state', 'axis', 'status', 'error_message', 'updated_at')[:20]
        )

        return Response({
            'credentials': {
                'total': HushraCredentials.objects.count(),
                'active': HushraCredentials.objects.filter(is_active=True).count(),
                'inactive': HushraCredentials.objects.filter(is_active=False).count(),
            },
            'jobs': {
                'total': SearchJob.objects.count(),
                'running': SearchJob.objects.filter(status='RUNNING').count(),
            },
            'tasks': status_counts,
            'latest_tasks': latest_tasks,
        })


# ---------------------------------------------------------------------------

class HushraCredentialsViewSet(viewsets.ModelViewSet):
    queryset = HushraCredentials.objects.all().order_by('-updated_at')
    serializer_class = HushraCredentialsSerializer

    @action(detail=False, methods=['post'])
    def bulk_add(self, request):
        """
        Accepts a newline-separated blob of UUIDs and bulk-creates credentials.
        Silently skips duplicates.
        Payload: { "uuids": "uuid1\nuuid2\n..." }
        """
        raw = request.data.get('uuids', '')
        uuids = [u.strip() for u in raw.splitlines() if u.strip()]
        if not uuids:
            return Response({"error": "No UUIDs provided."}, status=status.HTTP_400_BAD_REQUEST)

        objects = [HushraCredentials(uuid=u) for u in uuids]
        created = HushraCredentials.objects.bulk_create(objects, ignore_conflicts=True)
        return Response({"message": f"Added {len(created)} credentials (duplicates skipped)."}, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=['post'])
    def reset(self, request, pk=None):
        """Manually unblock a credential and reset its request counter."""
        credential = self.get_object()
        credential.reset()
        return Response({"message": f"Credential {credential.uuid[:8]}... reset to active."})

    @action(detail=False, methods=['post'])
    def reset_pool(self, request):
        """Bulk reset all credentials."""
        from django.utils import timezone
        count = HushraCredentials.objects.update(
            is_active=True, 
            request_count=0, 
            rate_limit_reset_time=None,
            updated_at=timezone.now()
        )
        return Response({"message": f"Successfully reset {count} credentials in the pool.", "count": count})

    @action(detail=True, methods=['get'])
    def test(self, request, pk=None):
        """Fire a real login attempt and return success/failure inline."""
        credential = self.get_object()
        client = HushraAPIClient()
        success = client.login(credential.uuid)
        return Response({"uuid": credential.uuid[:8] + "...", "login_success": success})


# ---------------------------------------------------------------------------
# Proxies
# ---------------------------------------------------------------------------

class ProxyViewSet(viewsets.ModelViewSet):
    queryset = Proxy.objects.all().order_by('-created_at')
    serializer_class = ProxySerializer

    @action(detail=True, methods=['post'])
    def toggle(self, request, pk=None):
        """Flip the is_active flag on a proxy."""
        proxy = self.get_object()
        proxy.is_active = not proxy.is_active
        proxy.save(update_fields=['is_active'])
        return Response({"id": proxy.id, "is_active": proxy.is_active})

    @action(detail=False, methods=['post'])
    def bulk_add(self, request):
        """
        Accepts a newline-separated blob of proxy URLs.
        Payload: { "proxies": "http://user:pass@host:port\n..." }
        """
        raw = request.data.get('proxies', '')
        urls = [u.strip() for u in raw.splitlines() if u.strip()]
        if not urls:
            return Response({"error": "No proxy URLs provided."}, status=status.HTTP_400_BAD_REQUEST)

        objects = [Proxy(url=u) for u in urls]
        created = Proxy.objects.bulk_create(objects, ignore_conflicts=True)
        return Response({"message": f"Added {len(created)} proxies (duplicates skipped)."}, status=status.HTTP_201_CREATED)


# ---------------------------------------------------------------------------
# State Runs
# ---------------------------------------------------------------------------

class StateRunViewSet(viewsets.ModelViewSet):
    queryset = StateRun.objects.all().order_by('-created_at')
    serializer_class = StateRunSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['job', 'state', 'status', 'priority']

    @action(detail=True, methods=['post'])
    def pause(self, request, pk=None):
        """Pause a state run - stop seeding new tasks but let current ones finish."""
        state_run = self.get_object()
        state_run.status = 'PAUSED'
        state_run.save(update_fields=['status'])
        return Response({"message": f"State {state_run.state} paused."})

    @action(detail=True, methods=['post'])
    def resume(self, request, pk=None):
        """Resume a paused state run."""
        state_run = self.get_object()
        state_run.status = 'RUNNING'
        state_run.save(update_fields=['status'])
        return Response({"message": f"State {state_run.state} resumed."})

    @action(detail=False, methods=['post'])
    def bulk_control(self, request):
        """Bulk pause/resume/stop control for multiple state runs."""
        action_name = str(request.data.get('action', '')).strip().lower()
        ids = request.data.get('state_run_ids', [])
        if action_name not in {'pause', 'resume', 'stop'}:
            return Response({'error': 'action must be one of pause, resume, stop'}, status=400)
        if not isinstance(ids, list) or not ids:
            return Response({'error': 'state_run_ids must be a non-empty list'}, status=400)

        runs = list(StateRun.objects.filter(id__in=ids))
        if not runs:
            return Response({'error': 'No matching state runs found.'}, status=404)

        results = []
        for sr in runs:
            if action_name == 'pause':
                sr.status = 'PAUSED'
                sr.save(update_fields=['status', 'updated_at'])
                results.append({'id': sr.id, 'state': sr.state, 'status': sr.status, 'cancelled': 0, 'revoked': 0})
            elif action_name == 'resume':
                sr.status = 'RUNNING'
                sr.save(update_fields=['status', 'updated_at'])
                results.append({'id': sr.id, 'state': sr.state, 'status': sr.status, 'cancelled': 0, 'revoked': 0})
            else:
                from core.celery import app as celery_app
                sr.status = 'PAUSED'
                sr.save(update_fields=['status', 'updated_at'])
                active_tasks = sr.tasks.filter(status__in=['PENDING', 'IN_PROGRESS'])
                revoke_ids = list(active_tasks.exclude(celery_task_id__isnull=True).values_list('celery_task_id', flat=True))
                cancelled = active_tasks.update(status='STOPPED', updated_at=timezone.now())
                revoked = 0
                for tid in revoke_ids:
                    if tid and tid != 'PENDING_COMMIT':
                        celery_app.control.revoke(tid, terminate=True)
                        revoked += 1
                sr.update_metrics()
                results.append({'id': sr.id, 'state': sr.state, 'status': sr.status, 'cancelled': cancelled, 'revoked': revoked})

        return Response({'message': f'Bulk {action_name} applied to {len(results)} state runs.', 'results': results})

    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """Stop a state run and revoke all active celery tasks for that state."""
        from core.celery import app as celery_app
        state_run = self.get_object()

        state_run.status = 'PAUSED'
        state_run.save(update_fields=['status', 'updated_at'])

        active_tasks = state_run.tasks.filter(status__in=['PENDING', 'IN_PROGRESS'])
        revoke_ids = list(active_tasks.exclude(celery_task_id__isnull=True).values_list('celery_task_id', flat=True))
        count = active_tasks.update(status='STOPPED', updated_at=timezone.now())

        revoked = 0
        for tid in revoke_ids:
            if tid and tid != 'PENDING_COMMIT':
                celery_app.control.revoke(tid, terminate=True)
                revoked += 1

        state_run.update_metrics()
        return Response({
            "message": f"State {state_run.state} stopped. {count} tasks cancelled ({revoked} revoked).",
            "cancelled": count,
            "revoked": revoked,
        })

    @action(detail=True, methods=['post'])
    def refresh_metrics(self, request, pk=None):
        """Manually trigger metrics recalculation."""
        state_run = self.get_object()
        state_run.update_metrics()
        return Response(StateRunSerializer(state_run).data)

    @action(detail=True, methods=['post'])
    def reset(self, request, pk=None):
        """Reset a state run - delete all tasks and start fresh."""
        state_run = self.get_object()
        deleted_count, _ = state_run.tasks.all().delete()
        state_run.status = 'PENDING'
        state_run.total_primes = 0
        state_run.primes_completed = 0
        state_run.total_tasks = 0
        state_run.tasks_pending = 0
        state_run.tasks_in_progress = 0
        state_run.tasks_completed = 0
        state_run.tasks_failed = 0
        state_run.total_records = 0
        state_run.started_at = None
        state_run.completed_at = None
        state_run.save()
        return Response({"message": f"State {state_run.state} reset. Deleted {deleted_count} tasks."})

    @action(detail=True, methods=['get'])
    def records(self, request, pk=None):
        """Return paginated PersonRecord rows for this state run."""
        state_run = self.get_object()

        try:
            page = max(1, int(request.query_params.get('page', 1)))
        except (TypeError, ValueError):
            page = 1

        try:
            page_size = int(request.query_params.get('page_size', 25))
        except (TypeError, ValueError):
            page_size = 25
        page_size = min(max(page_size, 1), 100)

        queryset = PersonRecord.objects.filter(task__state_run=state_run).order_by('-created_at', '-id')
        paginator = Paginator(queryset, page_size)
        page_obj = paginator.get_page(page)
        serializer = PersonRecordSerializer(page_obj.object_list, many=True)

        return Response({
            'count': paginator.count,
            'page': page_obj.number,
            'total_pages': paginator.num_pages,
            'page_size': page_size,
            'state_run_id': state_run.id,
            'state': state_run.state,
            'results': serializer.data,
        })


# ---------------------------------------------------------------------------
# Jobs & Tasks
# ---------------------------------------------------------------------------

class SearchJobViewSet(viewsets.ModelViewSet):
    queryset = SearchJob.objects.all().order_by('-created_at')
    serializer_class = SearchJobSerializer

    @action(detail=False, methods=['post'])
    def create_batch(self, request):
        """
        Accepts a list of targets and queues celery tasks for them.
        Payload: { "name": "My Bulk Job", "targets": [{"firstname": "John", "lastname": "Doe", "state": "CA"}, ...] }
        """
        name = request.data.get('name', 'Unnamed Job')
        targets = request.data.get('targets', [])

        if not targets:
            return Response({"error": "No targets provided."}, status=status.HTTP_400_BAD_REQUEST)

        job = SearchJob.objects.create(name=name)
        tasks_created = 0

        for target in targets:
            task = SearchTask.objects.create(
                job=job,
                firstname=target.get('firstname', ''),
                lastname=target.get('lastname', ''),
                state=target.get('state', ''),
            )
            res = execute_ssn_lookup.delay(task.id)
            task.celery_task_id = res.id
            task.save(update_fields=['celery_task_id'])
            tasks_created += 1

        return Response({"message": f"Queued {tasks_created} tasks.", "job_id": job.id}, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=['post'])
    def ignite_spider(self, request):
        """
        Takes a list of states and queues the initial 26 letters (A-Z) for each state.
        Optionally also starts a city-sweep pass for each state.
        Payload: { "name": "CA Spider Run", "states": ["CA", "NY"], "run_city_sweep": true }
        """
        name = request.data.get('name', 'Spider Job')
        states = request.data.get('states', [])
        run_city_sweep = request.data.get('run_city_sweep', False)

        if not states:
            return Response({"error": "No states provided."}, status=status.HTTP_400_BAD_REQUEST)

        job = SearchJob.objects.create(name=name)
        tasks_created = 0
        alphabet = "abcdefghijklmnopqrstuvwxyz"

        # Lastname-axis prime tasks
        for state in states:
            for letter in alphabet:
                task = SearchTask.objects.create(
                    job=job,
                    firstname="",
                    lastname=letter,
                    state=state,
                    axis='lastname',
                )
                res = execute_ssn_lookup.delay(task.id)
                task.celery_task_id = res.id
                task.save(update_fields=['celery_task_id'])
                tasks_created += 1

        # Optional city-sweep prime tasks
        if run_city_sweep:
            for state in states:
                for letter in alphabet:
                    task = SearchTask.objects.create(
                        job=job,
                        city=letter,
                        state=state,
                        axis='city',
                    )
                    res = execute_ssn_lookup.delay(task.id)
                    task.celery_task_id = res.id
                    task.save(update_fields=['celery_task_id'])
                    tasks_created += 1

        return Response({
            "message": f"Ignited Spider across {len(states)} states. Queued {tasks_created} prime tasks (city_sweep={'yes' if run_city_sweep else 'no'}).",
            "job_id": job.id,
        }, status=status.HTTP_201_CREATED)

    @action(detail=False, methods=['post'])
    def ignite_city_sweep(self, request):
        """
        Standalone city sweep: seeds A-Z city prefix tasks for each provided state.
        Useful for running the city axis independently of the lastname spider.
        Payload: { "name": "City Sweep CA", "states": ["CA", "TX"] }
        """
        name = request.data.get('name', 'City Sweep')
        states = request.data.get('states', [])

        if not states:
            return Response({"error": "No states provided."}, status=status.HTTP_400_BAD_REQUEST)

        job = SearchJob.objects.create(name=name)
        tasks_created = 0
        alphabet = "abcdefghijklmnopqrstuvwxyz"

        for state in states:
            for letter in alphabet:
                task = SearchTask.objects.create(
                    job=job,
                    city=letter,
                    state=state,
                    axis='city',
                )
                res = execute_ssn_lookup.delay(task.id)
                task.celery_task_id = res.id
                task.save(update_fields=['celery_task_id'])
                tasks_created += 1

        return Response({
            "message": f"City Sweep ignited across {len(states)} states. Queued {tasks_created} tasks.",
            "job_id": job.id,
        }, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=['post'])
    def resume(self, request, pk=None):
        """
        Re-queue only PENDING and FAILED tasks of an existing job without starting a new job.
        Useful for recovering partially completed spider runs.
        """
        job = self.get_object()
        pending_tasks = job.tasks.filter(status__in=['PENDING', 'FAILED', 'STOPPED', 'ABORTED'])
        count = pending_tasks.count()
        for task in pending_tasks:
            task.status = 'PENDING'
            task.save(update_fields=['status'])
            res = execute_ssn_lookup.delay(task.id)
            task.celery_task_id = res.id
            task.save(update_fields=['celery_task_id'])
        return Response({"message": f"Resumed {count} tasks for job '{job.name}'."})

    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """
        Revokes all pending/in_progress tasks for this job.
        Optimized to avoid timeouts with large jobs.
        """
        from core.celery import app as celery_app
        job = self.get_object()
        
        # 1. Mark the Job itself as STOPPED (prevents recursive spawning in workers)
        job.status = 'STOPPED'
        job.save(update_fields=['status'])
        
        # 2. Bulk update all tasks to STOPPED in one query
        active_tasks_query = job.tasks.filter(status__in=['PENDING', 'IN_PROGRESS'])
        
        # Capture celery IDs for IN_PROGRESS tasks before we update them
        # We only really need to revoke IN_PROGRESS ones. 
        # PENDING ones will just exit when they reach the status check.
        in_progress_ids = list(active_tasks_query.filter(status='IN_PROGRESS').values_list('celery_task_id', flat=True))
        
        # Perform the bulk update
        count = active_tasks_query.update(status='STOPPED', updated_at=timezone.now())
        
        # 3. Revoke currently running tasks
        revoked_running = 0
        for tid in in_progress_ids:
            if tid:
                celery_app.control.revoke(tid, terminate=True)
                revoked_running += 1
                
        return Response({
            "message": f"Job '{job.name}' stopped. {count} tasks cancelled ({revoked_running} running).",
            "count": count
        })


class SearchTaskViewSet(viewsets.ModelViewSet):
    queryset = SearchTask.objects.all().order_by('-created_at')
    serializer_class = SearchTaskSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['job', 'status', 'state']

    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        from core.celery import app as celery_app
        task = self.get_object()
        if task.status in ['PENDING', 'IN_PROGRESS']:
            if task.celery_task_id:
                celery_app.control.revoke(task.celery_task_id, terminate=True)
            task.status = 'STOPPED'
            task.save(update_fields=['status'])
            return Response({"message": "Task stopped."})
        return Response({"error": "Task is not running."}, status=status.HTTP_400_BAD_REQUEST)


class PersonRecordViewSet(viewsets.ModelViewSet):
    queryset = PersonRecord.objects.all().order_by('-created_at')
    serializer_class = PersonRecordSerializer
    filter_backends = [django_filters.rest_framework.DjangoFilterBackend]
    filterset_fields = ['task', 'state']


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class StatsView(APIView):
    def get(self, request):
        now = timezone.now()
        one_hour_ago = now - timedelta(hours=1)
        data = {
            "total_records": PersonRecord.objects.count(),
            "pending_tasks": SearchTask.objects.filter(status='PENDING').count(),
            "in_progress_tasks": SearchTask.objects.filter(status='IN_PROGRESS').count(),
            "active_credentials": HushraCredentials.objects.filter(is_active=True).count(),
            "total_credentials": HushraCredentials.objects.count(),
            "active_proxies": Proxy.objects.filter(is_active=True).count(),
            "total_proxies": Proxy.objects.count(),
            "records_last_hour": PersonRecord.objects.filter(created_at__gte=one_hour_ago).count(),
        }
        return Response(data)


# ---------------------------------------------------------------------------
# CSV/JSONL Export
# ---------------------------------------------------------------------------

def _record_rows():
    """Generator that yields CSV rows for all PersonRecord entries (streaming)."""
    header = ['id', 'firstname', 'middlename', 'lastname', 'ssn', 'dob', 'address', 'city', 'state', 'zip_code', 'phone', 'created_at']
    yield header
    qs = PersonRecord.objects.all().order_by('id').values_list(
        'id', 'firstname', 'middlename', 'lastname', 'ssn', 'dob',
        'address', 'city', 'state', 'zip_code', 'phone', 'created_at'
    ).iterator(chunk_size=500)
    for row in qs:
        yield list(row)


class ExportCSVView(APIView):
    def get(self, request):
        state_filter = request.query_params.get('state', None)

        def generate():
            import io
            if state_filter:
                qs = PersonRecord.objects.filter(state=state_filter.upper()).order_by('id').values_list(
                    'id', 'firstname', 'middlename', 'lastname', 'ssn', 'dob',
                    'address', 'city', 'state', 'zip_code', 'phone', 'created_at'
                ).iterator(chunk_size=500)
            else:
                qs = PersonRecord.objects.all().order_by('id').values_list(
                    'id', 'firstname', 'middlename', 'lastname', 'ssn', 'dob',
                    'address', 'city', 'state', 'zip_code', 'phone', 'created_at'
                ).iterator(chunk_size=500)

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(['id', 'firstname', 'middlename', 'lastname', 'ssn', 'dob', 'address', 'city', 'state', 'zip_code', 'phone', 'created_at'])
            yield buf.getvalue()
            buf.truncate(0)
            buf.seek(0)

            for row in qs:
                writer.writerow(row)
                yield buf.getvalue()
                buf.truncate(0)
                buf.seek(0)

        filename = f"records_{state_filter.upper() if state_filter else 'all'}.csv"
        response = StreamingHttpResponse(generate(), content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
