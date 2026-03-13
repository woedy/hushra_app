import random
import time
import logging
import requests
from django.conf import settings
from django.core.cache import cache
from celery import shared_task
from django.utils import timezone

from django.db import transaction
import json

from .models import HushraCredentials, SearchJob, SearchTask, PersonRecord, Proxy, GlobalSetting, StateRun
from .hushra_client import HushraAPIClient

logger = logging.getLogger(__name__)

TOKEN_CACHE_TTL = getattr(settings, "HUSHRA_TOKEN_CACHE_TTL", 3600)

# The depth at which a lastname axis switches to firstname axis for finer drilling
LASTNAME_DEPTH_THRESHOLD = getattr(settings, "HUSHRA_LASTNAME_DEPTH_THRESHOLD", 6)

# Alphabet for prefix expansion
ALPHABET = "abcdefghijklmnopqrstuvwxyz"


def _get_client_with_cached_token(credential):
    """
    Returns an authenticated HushraAPIClient, reusing a cached Redis token
    for the given credential to avoid re-logging in on every task.
    Implements proxy failover: if login fails, retries with a fresh proxy.
    Returns: (client, was_cached, last_error_code)
    """
    cache_key = f"hushra_token_{credential.id}"
    cached_token = cache.get(cache_key)

    # Check if we should use proxies at all
    use_proxy = GlobalSetting.get_value('use_proxy', default=True)

    # Try up to 3 times to get an authenticated client (in case of bad proxies)
    max_proxy_retries = 3 if use_proxy else 1
    last_err = 'SUCCESS'

    for attempt in range(max_proxy_retries):
        proxy_url = None
        current_proxy_obj = None

        if use_proxy:
            # Get a random active proxy record so we can mark it failed if needed
            current_proxy_obj = Proxy.objects.filter(is_active=True).order_by("?").first()
            if current_proxy_obj:
                proxy_url = current_proxy_obj.url
                current_proxy_obj.last_used = timezone.now()
                current_proxy_obj.save(update_fields=['last_used'])

        client = HushraAPIClient(proxy=proxy_url)

        if attempt == 0 and cached_token:
            logger.info(f"[REUSING TOKEN] Credential {credential.uuid[:8]}... from cache.")
            client.set_token(cached_token)
            return client, True, 'SUCCESS'

        # Fresh login
        last_err = client.login(credential.uuid)
        if last_err == "SUCCESS":
            cache.set(cache_key, client.token, TOKEN_CACHE_TTL)
            return client, False, 'SUCCESS'

        # If login failed and we were using a proxy, mark that proxy as suspicious
        if use_proxy and current_proxy_obj and last_err != "AUTH_FAILED":
            logger.warning(f"Login failed ({last_err}) via proxy {proxy_url[:30]}... marking as failed.")
            current_proxy_obj.mark_failed(max_fails=3)
            # Short sleep before trying next proxy
            time.sleep(1)
        elif last_err == "AUTH_FAILED":
            # If it's a real auth failure, don't bother trying other proxies
            break
        else:
            # If not using proxy or generic failure, just fail early
            break

    return None, False, last_err


def _do_lookup(client, task):
    """
    Performs the API lookup for a task, dispatching the right fields based on task.axis.
    Returns the raw results list.
    """
    axis = task.axis

    if axis == 'city':
        # City sweep: use city + state, no name filters
        return client.lookup(
            city=task.city,
            state=task.state,
        )
    elif axis == 'firstname':
        # Firstname expansion: lastname fixed, expanding on firstname prefix
        return client.lookup(
            firstname=task.firstname,
            lastname=task.lastname,
            state=task.state,
        )
    else:
        # Default: lastname axis (original behavior)
        return client.lookup(
            firstname=task.firstname,
            lastname=task.lastname,
            state=task.state,
        )


def _spawn_children(task, axis, base_prefix, prefix_field, extra_fields=None):
    """
    Spawns 26 child tasks (one per letter) for the given axis and base prefix.
    Skips already-completed combinations.
    Returns the count of spawned tasks.
    """
    extra_fields = extra_fields or {}
    spawned = 0

    for letter in ALPHABET:
        child_prefix = base_prefix + letter

        # Dedup check: skip if this prefix+state combo is already done for this axis
        filter_kwargs = {
            'axis': axis,
            'state': task.state,
            'status__in': ['COMPLETED', 'TOO_BROAD'],
        }
        filter_kwargs[prefix_field] = child_prefix

        # Add any extra fields (e.g. locked lastname when expanding firstname)
        for k, v in extra_fields.items():
            filter_kwargs[k] = v

        already_done = SearchTask.objects.filter(**filter_kwargs).exists()
        if already_done:
            continue

        create_kwargs = {
            'job': task.job,
            'axis': axis,
            'state': task.state,
            'status': 'PENDING',
            'firstname': '',
            'lastname': '',
            'city': '',
        }
        create_kwargs[prefix_field] = child_prefix
        for k, v in extra_fields.items():
            create_kwargs[k] = v

        new_task = SearchTask.objects.create(**create_kwargs)
        # Use on_commit to ensure the task isn't picked up before the DB has it
        transaction.on_commit(lambda task_id=new_task.id: execute_ssn_lookup.delay(task_id))
        new_task.celery_task_id = "PENDING_COMMIT"
        new_task.save(update_fields=['celery_task_id'])
        spawned += 1

    return spawned


@shared_task(bind=True, max_retries=5, rate_limit='30/m')
def execute_ssn_lookup(self, task_id):
    """
    Background task to execute a single SSN lookup via HushraAPIClient.
    Implements:
      - Rate limiting, credential pooling, jitter, backoff
      - Token caching, proxy rotation
      - Stale session detection: if a cached token yields 0 results, re-auth and retry once
      - Multi-axis expansion: lastname → firstname (depth threshold) → city sweep
    """
    try:
        task = SearchTask.objects.get(id=task_id)
        logger.info(f"Task {task_id} FOUND in DB. Status: {task.status}, Job: {task.job.name}")
    except SearchTask.DoesNotExist:
        # Possible transaction race — wait a moment and try one last time
        time.sleep(1.0)
        try:
            task = SearchTask.objects.get(id=task_id)
            logger.info(f"Task {task_id} FOUND on retry after short sleep.")
        except SearchTask.DoesNotExist:
            logger.error(f"CRITICAL: SearchTask {task_id} GONE from DB even after retry.")
            return

    # 0. Check if task OR job has been stopped
    if task.status == 'STOPPED' or task.job.status == 'STOPPED':
        logger.info(f"Task {task_id} (Job {task.job_id}) was stopped before execution.")
        if task.status != 'STOPPED':
            task.status = 'STOPPED'
            task.save(update_fields=['status'])
        return

    # 1. Claim task atomically so duplicate deliveries don't re-run the same row
    claimed = SearchTask.objects.filter(id=task.id, status='PENDING').update(
        status='IN_PROGRESS',
        updated_at=timezone.now(),
    )
    if claimed == 0:
        logger.info(f"Task {task_id} skipped because status is already {task.status}.")
        return
    task.status = 'IN_PROGRESS'

    def _requeue_task(countdown):
        task.status = 'PENDING'
        task.save(update_fields=['status', 'updated_at'])
        raise self.retry(countdown=countdown)

    # 2. Jitter to avoid stampeding requests
    delay = random.uniform(settings.CELERY_MIN_JITTER, settings.CELERY_MAX_JITTER)
    logger.info(f"Task {task_id} [{task.axis}] sleeping {delay:.2f}s jitter.")
    time.sleep(delay)

    # 3. Pull a healthy credential (under dynamic soft request limit)
    soft_limit = GlobalSetting.get_value('soft_limit', 80)
    credential = HushraCredentials.get_available_credential(soft_limit=soft_limit)
    if not credential:
        logger.warning("No active Hushra credentials available. Retrying in 5 min.")
        _requeue_task(getattr(settings, "HUSHRA_NO_CREDENTIAL_RETRY_SECONDS", 300))

    # 4. Get authenticated client (with token caching & failover)
    cache_key = f"hushra_token_{credential.id}"
    client, was_cached, login_err = _get_client_with_cached_token(credential)

    if client is None:
        if login_err == "AUTH_FAILED":
            logger.error(f"AUTH_FAILED for UUID {credential.uuid[:8]}... Marking exhausted.")
            cache.delete(cache_key)
            credential.mark_rate_limited(hours=24)
            _requeue_task(getattr(settings, "HUSHRA_AUTH_FAILED_RETRY_SECONDS", 10))
        elif login_err == "RATE_LIMITED":
            logger.warning(f"RATE_LIMITED during login for UUID {credential.uuid[:8]}...")
            cache.delete(cache_key)
            credential.mark_rate_limited(hours=1)
            _requeue_task(getattr(settings, "HUSHRA_RATE_LIMIT_RETRY_SECONDS", 300))
        else:
            logger.warning(f"System error ({login_err}) during login for UUID {credential.uuid[:8]}... Retrying.")
            cache.delete(cache_key)
            backoff_cap = getattr(settings, "HUSHRA_RATE_LIMIT_RETRY_SECONDS", 300)
            _requeue_task(min(backoff_cap, (2 ** self.request.retries) * 30))

    # 5. Increment the soft usage counter
    credential.increment_request_count()

    # 6. Execute the lookup
    try:
        results = _do_lookup(client, task)

        # ── STALE SESSION DETECTION ───────────────────────────────────────────
        # If we used a cached token and got 0 results the session may have expired.
        # Bust the cache, re-login, and retry the lookup exactly ONCE.
        if len(results) == 0 and was_cached:
            logger.warning(
                f"[STALE TOKEN?] Task {task_id} got 0 results from cached token for "
                f"credential {credential.uuid[:8]}... Busting cache and re-authenticating."
            )
            cache.delete(cache_key)
            fresh_client, _, fresh_err = _get_client_with_cached_token(credential)
            if fresh_client:
                retry_results = _do_lookup(fresh_client, task)
                if len(retry_results) > 0:
                    logger.info(
                        f"[STALE TOKEN RECOVERED] Task {task_id} got {len(retry_results)} "
                        f"results after re-auth. Stale session confirmed."
                    )
                    results = retry_results
                else:
                    logger.info(
                        f"[GENUINE EMPTY] Task {task_id} confirmed 0 results after re-auth."
                    )
            else:
                logger.warning(
                    f"[RE-AUTH FAILED] Task {task_id}: could not get fresh client ({fresh_err}). "
                    f"Treating as 0 results."
                )
        # ─────────────────────────────────────────────────────────────────────

        LIMIT_THRESHOLD = getattr(settings, "HUSHRA_LOOKUP_LIMIT_THRESHOLD", 50)

        if len(results) >= LIMIT_THRESHOLD:
            # Result set was truncated — need to go deeper
            _save_records(results, task)

            # Check if Job was stopped while we were doing the lookup
            task.job.refresh_from_db()
            if task.job.status == 'STOPPED':
                logger.info(f"Job {task.job_id} was stopped during lookup. Not spawning children.")
                task.status = 'STOPPED'
                task.save(update_fields=['status'])
                return "Job stopped during lookup."

            # ── AXIS-AWARE EXPANSION ──────────────────────────────────────────
            axis = task.axis
            spawned = 0

            if axis == 'lastname':
                lastname_depth = len(task.lastname)
                if lastname_depth < LASTNAME_DEPTH_THRESHOLD:
                    # Continue expanding on lastname
                    logger.info(
                        f"Task {task_id}: lastname depth={lastname_depth}, "
                        f"spawning 26 lastname children for '{task.lastname}'."
                    )
                    spawned = _spawn_children(
                        task, axis='lastname',
                        base_prefix=task.lastname,
                        prefix_field='lastname',
                        extra_fields={'firstname': task.firstname},
                    )
                else:
                    # Lastname is deep enough — switch to firstname axis
                    logger.info(
                        f"Task {task_id}: lastname depth={lastname_depth} >= threshold={LASTNAME_DEPTH_THRESHOLD}. "
                        f"[AXIS SWITCH] Spawning firstname children for lastname='{task.lastname}'."
                    )
                    spawned = _spawn_children(
                        task, axis='firstname',
                        base_prefix='',  # firstname starts fresh from each letter
                        prefix_field='firstname',
                        extra_fields={'lastname': task.lastname},
                    )

            elif axis == 'firstname':
                firstname_depth = len(task.firstname)
                if firstname_depth < LASTNAME_DEPTH_THRESHOLD:
                    # Continue expanding on firstname with the locked lastname
                    logger.info(
                        f"Task {task_id}: firstname depth={firstname_depth}, "
                        f"spawning 26 firstname children for firstname='{task.firstname}' lastname='{task.lastname}'."
                    )
                    spawned = _spawn_children(
                        task, axis='firstname',
                        base_prefix=task.firstname,
                        prefix_field='firstname',
                        extra_fields={'lastname': task.lastname},
                    )
                else:
                    # Too deep on both axes — mark and move on
                    logger.warning(
                        f"Task {task_id}: firstname depth={firstname_depth} >= threshold. "
                        f"Both axes exhausted for '{task.firstname} {task.lastname}'. Marking TOO_BROAD."
                    )

            elif axis == 'city':
                city_depth = len(task.city)
                if city_depth < LASTNAME_DEPTH_THRESHOLD:
                    logger.info(
                        f"Task {task_id}: city depth={city_depth}, "
                        f"spawning 26 city children for city='{task.city}'."
                    )
                    spawned = _spawn_children(
                        task, axis='city',
                        base_prefix=task.city,
                        prefix_field='city',
                    )
                else:
                    logger.warning(
                        f"Task {task_id}: city depth={city_depth} exhausted for city='{task.city}'. Marking TOO_BROAD."
                    )
            # ─────────────────────────────────────────────────────────────────

            task.status = 'TOO_BROAD'
            task.error_message = f"Hit limit of {LIMIT_THRESHOLD} on axis={axis}, spawned {spawned} deeper tasks."
            task.save(update_fields=['status', 'error_message', 'updated_at'])
            return f"Task {task_id} spawned {spawned} children on axis={axis}."

        else:
            _save_records(results, task)
            task.status = 'COMPLETED'
            task.save(update_fields=['status', 'updated_at'])
            return f"Task {task_id} completed with {len(results)} matches (axis={task.axis})."

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            logger.warning(f"Credential {credential.uuid[:8]}... hit rate limit (429).")
            cache.delete(cache_key)
            credential.mark_rate_limited(hours=1)
            rate_cap = getattr(settings, "HUSHRA_RATE_LIMIT_RETRY_SECONDS", 300)
            countdown = min(rate_cap, (2 ** self.request.retries) * 10)
            _requeue_task(countdown)
        else:
            task.status = 'FAILED'
            task.error_message = f"HTTP Error: {e}"
            task.save(update_fields=['status', 'error_message', 'updated_at'])
            return f"Task {task_id} failed: {e}"

    except Exception as e:
        logger.exception(f"Unexpected error processing task {task_id}")
        if was_cached:
            cache.delete(cache_key)
        task.status = 'FAILED'
        task.error_message = str(e)
        task.save(update_fields=['status', 'error_message', 'updated_at'])
        return f"Task {task_id} failed: {e}"


def _save_records(results, task):
    """
    Bulk-save PersonRecord entries using INSERT OR IGNORE (ignore_conflicts).
    Falls back to individual creation for records without SSN using tiered dedup.
    """
    to_bulk = []
    for record_data in results:
        ssn = (record_data.get('ssn') or '').strip() or None
        firstname = (record_data.get('firstname') or '').strip()
        lastname = (record_data.get('lastname') or '').strip()
        zip_code = (record_data.get('zip') or '').strip()
        dob = (record_data.get('dob') or '').strip()
        phone = (record_data.get('phone') or '').strip()

        if ssn:
            to_bulk.append(PersonRecord(
                task=task,
                firstname=firstname or None,
                middlename=record_data.get('middlename') or None,
                lastname=lastname or None,
                ssn=ssn,
                dob=dob or None,
                address=record_data.get('address') or None,
                city=record_data.get('city') or None,
                state=record_data.get('st') or None,
                zip_code=zip_code or None,
                phone=phone or None,
                raw_data=record_data,
            ))
        else:
            # No SSN — use tiered in-DB dedup before creating
            if firstname and lastname:
                if dob and PersonRecord.objects.filter(firstname=firstname, lastname=lastname, dob=dob).exists():
                    continue
                if phone and PersonRecord.objects.filter(firstname=firstname, lastname=lastname, phone=phone).exists():
                    continue
                if zip_code and PersonRecord.objects.filter(firstname=firstname, lastname=lastname, zip_code=zip_code).exists():
                    continue

            PersonRecord.objects.create(
                task=task,
                firstname=firstname or None,
                middlename=record_data.get('middlename') or None,
                lastname=lastname or None,
                ssn=None,
                dob=dob or None,
                address=record_data.get('address') or None,
                city=record_data.get('city') or None,
                state=record_data.get('st') or None,
                zip_code=zip_code or None,
                phone=phone or None,
                raw_data=record_data,
            )

    if to_bulk:
        PersonRecord.objects.bulk_create(to_bulk, ignore_conflicts=True)
        logger.info(f"Bulk-created {len(to_bulk)} records for task {task.id} (duplicates skipped).")


@shared_task
@transaction.atomic
def orchestrate_spider(seed_anyway=False):
    """
    Periodic task run by Celery Beat (e.g., every 1 minute).
    Enforces the 'auto_run_enabled' setting. If ON, it seeds the queue with
    new A-Z prime tasks up to the 'auto_queue_min' threshold.
    If seed_anyway=True (triggered from seed_now endpoint), ignores enabled check.
    """
    lock_expire = 60 * 5  # 5 minute safety lock
    lock_id = "orchestrate_spider_lock"
    
    # Try to acquire lock
    if not cache.add(lock_id, "true", lock_expire):
        logger.info("Orchestrator: Already running. Skipping this tick.")
        return "Already running."

    try:
        enabled = GlobalSetting.get_value('auto_run_enabled', default=False)
        if not seed_anyway and str(enabled).lower() != 'true':
            return "Auto Run is disabled."

        min_queue = int(GlobalSetting.get_value('auto_queue_min', default=500))

        # How many pending tasks do we have?
        pending_count = SearchTask.objects.filter(status__in=['PENDING', 'IN_PROGRESS']).count()

        if pending_count >= min_queue:
            return f"Queue healthy ({pending_count} >= {min_queue}). No seeding needed."

        to_seed = min_queue - pending_count

        # Which states and axes?
        states_str = GlobalSetting.get_value('auto_run_states', default='')
        axes_str = GlobalSetting.get_value('auto_run_axes', default='lastname')

        if not states_str:
            return "Auto Run is enabled but no states are configured."

        try:
            states = json.loads(states_str) if '[' in states_str else [s.strip() for s in states_str.split(',') if s.strip()]
        except Exception:
            states = [s.strip() for s in states_str.split(',') if s.strip()]

        axes = [a.strip() for a in axes_str.split(',') if a.strip()]

        if not states or not axes:
            return "Invalid states or axes configuration."

        if HushraCredentials.objects.count() == 0:
            return "No UUID credentials configured. Add UUIDs before running Auto Run."

        soft_limit = GlobalSetting.get_value('soft_limit', 80)
        if not HushraCredentials.has_usable_credentials(soft_limit=soft_limit):
            return "No usable UUID credentials available. Reset UUID pool or increase soft limit."

        # Use a persistent Job for Auto Orchestration to group them
        job, _ = SearchJob.objects.get_or_create(name="Auto Orchestrator Job", defaults={'status': 'RUNNING'})
        if job.status == 'STOPPED':
            # If someone manually stopped it, reset it so we can use it again
            job.status = 'RUNNING'
            job.save(update_fields=['status'])

        seeded = 0
        state_runs = {}

        # We iterate over states and axes, looking for A-Z primes that don't exist yet
        for state in states:
            normalized_state = state.upper()
            state_run, _ = StateRun.objects.get_or_create(
                job=job,
                state=normalized_state,
                defaults={
                    'status': 'RUNNING',
                    'axes_enabled': axes,
                },
            )
            if state_run.status in ['COMPLETED', 'FAILED']:
                state_run.status = 'RUNNING'
            state_run.axes_enabled = axes
            state_run.started_at = state_run.started_at or timezone.now()
            state_run.save(update_fields=['status', 'axes_enabled', 'started_at', 'updated_at'])
            state_runs[normalized_state] = state_run

            for axis in axes:
                for letter in ALPHABET:
                    if seeded >= to_seed:
                        break  # Hit our target

                    # Check if this exact prime task already exists (completed, pending, failed)
                    # Prime tasks have firstname='', lastname=letter (for lastname axis) or city=letter (for city axis)
                    filter_kwargs = {
                        'axis': axis,
                        'state': normalized_state
                    }
                    if axis == 'lastname':
                        filter_kwargs['lastname'] = letter
                        filter_kwargs['firstname'] = ''
                    elif axis == 'city':
                        filter_kwargs['city'] = letter
                    elif axis == 'firstname':
                        filter_kwargs['firstname'] = letter
                        filter_kwargs['lastname'] = ''
                    else:
                        continue # Ignore unknown axes

                    # Only skip if this exact prime task is PENDING or IN_PROGRESS already.
                    # COMPLETED, FAILED, STOPPED tasks are prime candidates for re-seeding.
                    exists = SearchTask.objects.filter(
                        **filter_kwargs, status__in=['PENDING', 'IN_PROGRESS']
                    ).exists()

                    if not exists:
                        # Delete any stale COMPLETED/FAILED/STOPPED version so DB stays clean
                        stale_qs = SearchTask.objects.filter(**filter_kwargs).exclude(
                            status__in=['PENDING', 'IN_PROGRESS']
                        )
                        deleted_count = stale_qs.count()
                        if deleted_count > 0:
                            logger.info(f"Orchestrator: Deleting {deleted_count} stale tasks for {filter_kwargs}")
                            stale_qs.delete()

                        create_kwargs = filter_kwargs.copy()
                        create_kwargs['job'] = job
                        create_kwargs['status'] = 'PENDING'
                        create_kwargs['state_run'] = state_runs[normalized_state]

                        if 'firstname' not in create_kwargs: create_kwargs['firstname'] = ''
                        if 'lastname' not in create_kwargs: create_kwargs['lastname'] = ''
                        if 'city' not in create_kwargs: create_kwargs['city'] = ''

                        task = SearchTask.objects.create(**create_kwargs)
                        logger.info(f"Orchestrator: CREATED Task {task.id} for {filter_kwargs}")
                        # Use on_commit to ensure the worker doesn't start until the row is persistent
                        transaction.on_commit(lambda task_id=task.id: execute_ssn_lookup.delay(task_id))
                        task.celery_task_id = "PENDING_COMMIT"
                        task.save(update_fields=['celery_task_id'])
                        seeded += 1
                
                if seeded >= to_seed:
                    break
            if seeded >= to_seed:
                break

        for state_run in state_runs.values():
            state_run.update_metrics()

        return f"Orchestrator: Queued {seeded} new prime tasks across {len(states)} states."

    finally:
        # Release the lock
        cache.delete(lock_id)
