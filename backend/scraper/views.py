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
        min_queue = GlobalSetting.get_value('auto_queue_min', default='500')
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

        soft_limit = GlobalSetting.get_value('soft_limit', 80)
        if not HushraCredentials.has_usable_credentials(soft_limit=soft_limit):
            return Response({'error': 'No active UUID credentials are available right now. Reset the UUID pool or add new UUIDs.'}, status=400)

        # Run one orchestration cycle inline so the UI immediately shows seeded jobs/tasks,
        # then rely on worker execution for background lookup processing.
        result = orchestrate_spider(seed_anyway=True)
        return Response({'message': f'Seeding triggered successfully. {result}'})

    @action(detail=False, methods=['post'])
    def new_session(self, request):
        """
        Reset the auto-orchestrator for a new sweep session.
        Deletes all COMPLETED, FAILED, STOPPED prime tasks on the Auto Orchestrator Job
        so the next tick (or Seed Now) will re-seed a full A-Z sweep from scratch.
        """
        from .models import SearchJob
        try:
            job = SearchJob.objects.get(name="Auto Orchestrator Job")
            deleted_count, _ = job.tasks.filter(
                status__in=['COMPLETED', 'FAILED', 'STOPPED', 'ABORTED', 'TOO_BROAD']
            ).delete()
            # Reset job to RUNNING so it can accept new tasks
            job.status = 'RUNNING'
            job.save(update_fields=['status'])

            # PURGE CELERY QUEUE
            # This clears out any "ghost" tasks that are pointing to deleted rows.
            from core.celery import app as celery_app
            with celery_app.connection_or_acquire() as conn:
                conn.default_channel.queue_purge('celery')

            return Response({'message': f'New session started. Cleared {deleted_count} tasks and purged the queue.', 'deleted': deleted_count})
        except SearchJob.DoesNotExist:
            return Response({'message': 'No existing auto-orchestrator job. Ready for first run.', 'deleted': 0})


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

    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """Stop a state run and revoke all its tasks."""
        from core.celery import app as celery_app
        state_run = self.get_object()
        
        # Mark state run as stopped
        state_run.status = 'FAILED'
        state_run.save(update_fields=['status'])
        
        # Stop all active tasks for this state
        active_tasks = state_run.tasks.filter(status__in=['PENDING', 'IN_PROGRESS'])
        in_progress_ids = list(active_tasks.filter(status='IN_PROGRESS').values_list('celery_task_id', flat=True))
        
        count = active_tasks.update(status='STOPPED', updated_at=timezone.now())
        
        # Revoke running tasks
        for tid in in_progress_ids:
            if tid:
                celery_app.control.revoke(tid, terminate=True)
        
        return Response({"message": f"State {state_run.state} stopped. {count} tasks cancelled."})

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
