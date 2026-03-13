from django.db import models
from django.utils import timezone


class GlobalSetting(models.Model):
    key = models.CharField(max_length=100, unique=True)
    value = models.CharField(max_length=255, default='true')
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.key}: {self.value}"

    @classmethod
    def get_value(cls, key, default=True):
        obj, created = cls.objects.get_or_create(key=key, defaults={'value': str(default).lower()})
        val = obj.value.lower()
        if val == 'true': return True
        if val == 'false': return False
        try:
            return int(val)
        except:
            return obj.value


class HushraCredentials(models.Model):
    uuid = models.CharField(max_length=255, unique=True)
    is_active = models.BooleanField(default=True)
    rate_limit_reset_time = models.DateTimeField(null=True, blank=True)
    request_count = models.IntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        status = "Active" if self.is_active else "Exhausted"
        return f"UUID {self.uuid[:8]}... ({status})"

    @classmethod
    def restore_ready_credentials(cls):
        """Re-activate credentials whose cooldown window has passed."""
        now = timezone.now()
        cls.objects.filter(is_active=False, rate_limit_reset_time__lte=now).update(
            is_active=True, rate_limit_reset_time=None
        )

    @classmethod
    def has_usable_credentials(cls, soft_limit=80):
        """True if at least one credential can be scheduled right now."""
        cls.restore_ready_credentials()
        return cls.objects.filter(is_active=True, request_count__lt=soft_limit).exists()

    @classmethod
    def get_available_credential(cls, soft_limit=80):
        """Returns a random active credential that is not rate limited and under the soft request limit."""
        cls.restore_ready_credentials()
        return (
            cls.objects.filter(is_active=True, request_count__lt=soft_limit)
            .order_by("?")
            .first()
        )

    def mark_rate_limited(self, hours=1):
        """Mark this credential as temporarily blocked."""
        self.is_active = False
        self.rate_limit_reset_time = timezone.now() + timezone.timedelta(hours=hours)
        self.save(update_fields=['is_active', 'rate_limit_reset_time', 'updated_at'])

    def increment_request_count(self):
        HushraCredentials.objects.filter(pk=self.pk).update(
            request_count=models.F('request_count') + 1
        )

    def reset(self):
        """Manually unblock and reset request count."""
        self.is_active = True
        self.rate_limit_reset_time = None
        self.request_count = 0
        self.save(update_fields=['is_active', 'rate_limit_reset_time', 'request_count', 'updated_at'])


class Proxy(models.Model):
    url = models.CharField(max_length=255, unique=True)  # e.g. http://user:pass@host:port
    is_active = models.BooleanField(default=True)
    fail_count = models.IntegerField(default=0)
    last_used = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.url

    @classmethod
    def get_random_active(cls):
        """Returns a random active proxy URL string, or None if pool is empty."""
        proxy = cls.objects.filter(is_active=True).order_by("?").first()
        if proxy:
            proxy.last_used = timezone.now()
            proxy.save(update_fields=['last_used'])
            return proxy.url
        return None

    def mark_failed(self, max_fails=3):
        """Increment failure counter and deactivate after threshold."""
        self.fail_count = models.F('fail_count') + 1
        self.save(update_fields=['fail_count'])
        self.refresh_from_db()
        if self.fail_count >= max_fails:
            self.is_active = False
            self.save(update_fields=['is_active'])

    class Meta:
        verbose_name_plural = "Proxies"


class SearchJob(models.Model):
    STATUS_CHOICES = [
        ('RUNNING', 'Running'),
        ('STOPPED', 'Stopped'),
        ('COMPLETED', 'Completed'),
    ]

    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='RUNNING')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name} ({self.status})"


class StateRun(models.Model):
    """Tracks the complete sweep of a single state across all configured axes."""
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('RUNNING', 'Running'),
        ('PAUSED', 'Paused'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]

    PRIORITY_CHOICES = [
        ('HIGH', 'High'),
        ('MEDIUM', 'Medium'),
        ('LOW', 'Low'),
    ]

    job = models.ForeignKey(SearchJob, related_name='state_runs', on_delete=models.CASCADE)
    state = models.CharField(max_length=2, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING', db_index=True)
    priority = models.CharField(max_length=10, choices=PRIORITY_CHOICES, default='MEDIUM')

    # Configuration
    axes_enabled = models.JSONField(default=list)  # ["lastname", "firstname", "city"]
    max_concurrent_tasks = models.IntegerField(default=100)
    min_queue_depth = models.IntegerField(default=50)

    # Progress tracking
    total_primes = models.IntegerField(default=0)
    primes_completed = models.IntegerField(default=0)
    total_tasks = models.IntegerField(default=0)
    tasks_pending = models.IntegerField(default=0)
    tasks_in_progress = models.IntegerField(default=0)
    tasks_completed = models.IntegerField(default=0)
    tasks_failed = models.IntegerField(default=0)
    total_records = models.IntegerField(default=0)

    # Performance metrics
    avg_task_duration = models.FloatField(default=0.0)  # seconds
    tasks_per_minute = models.FloatField(default=0.0)
    records_per_task = models.FloatField(default=0.0)
    duplicate_rate = models.FloatField(default=0.0)  # percentage

    # Timestamps
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    estimated_completion = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [['job', 'state']]
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['state', 'status']),
            models.Index(fields=['job', 'state']),
        ]

    def __str__(self):
        return f"{self.state} - {self.status} ({self.progress_percentage:.1f}%)"

    @property
    def progress_percentage(self):
        if self.total_primes == 0:
            return 0.0
        return (self.primes_completed / self.total_primes) * 100

    def update_metrics(self):
            """Recalculate all metrics based on current tasks."""
            from django.db.models import Count, Avg, Q
            from django.utils import timezone

            # Get all tasks for this state run
            tasks = SearchTask.objects.filter(job=self.job, state=self.state)

            # Count by status
            status_counts = tasks.aggregate(
                pending=Count('id', filter=Q(status='PENDING')),
                in_progress=Count('id', filter=Q(status='IN_PROGRESS')),
                completed=Count('id', filter=Q(status='COMPLETED')),
                failed=Count('id', filter=Q(status__in=['FAILED', 'STOPPED', 'ABORTED'])),
            )

            self.tasks_pending = status_counts['pending']
            self.tasks_in_progress = status_counts['in_progress']
            self.tasks_completed = status_counts['completed']
            self.tasks_failed = status_counts['failed']
            self.total_tasks = tasks.count()

            # Count primes (tasks with depth 1 on their axis)
            prime_tasks = tasks.filter(
                Q(axis='lastname', lastname__regex=r'^[a-z]$') |
                Q(axis='firstname', firstname__regex=r'^[a-z]$') |
                Q(axis='city', city__regex=r'^[a-z]$')
            )
            self.total_primes = prime_tasks.count()
            self.primes_completed = prime_tasks.filter(status__in=['COMPLETED', 'TOO_BROAD']).count()

            # Count records
            self.total_records = PersonRecord.objects.filter(task__job=self.job, task__state=self.state).count()

            # Calculate performance metrics
            completed_tasks = tasks.filter(status='COMPLETED', updated_at__isnull=False, created_at__isnull=False)
            if completed_tasks.exists():
                durations = [(t.updated_at - t.created_at).total_seconds() for t in completed_tasks[:100]]
                self.avg_task_duration = sum(durations) / len(durations) if durations else 0.0

                if self.total_tasks > 0:
                    self.records_per_task = self.total_records / self.total_tasks

            # Calculate tasks per minute
            if self.started_at:
                elapsed = (timezone.now() - self.started_at).total_seconds() / 60
                if elapsed > 0:
                    self.tasks_per_minute = self.tasks_completed / elapsed

            # Estimate completion
            if self.tasks_per_minute > 0 and self.tasks_pending > 0:
                minutes_remaining = self.tasks_pending / self.tasks_per_minute
                self.estimated_completion = timezone.now() + timezone.timedelta(minutes=minutes_remaining)

            # Update status based on progress
            if self.status == 'RUNNING':
                if self.tasks_pending == 0 and self.tasks_in_progress == 0:
                    if self.primes_completed >= self.total_primes:
                        self.status = 'COMPLETED'
                        self.completed_at = timezone.now()

            self.save(update_fields=[
                'tasks_pending', 'tasks_in_progress', 'tasks_completed', 'tasks_failed',
                'total_tasks', 'total_primes', 'primes_completed', 'total_records',
                'avg_task_duration', 'tasks_per_minute', 'records_per_task',
                'estimated_completion', 'status', 'completed_at', 'updated_at'
            ])





class SearchTask(models.Model):
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('IN_PROGRESS', 'In Progress'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
        ('TOO_BROAD', 'Too Broad'),
        ('STOPPED', 'Stopped'),
        ('ABORTED', 'Aborted'),
    ]

    AXIS_CHOICES = [
        ('lastname', 'Last Name'),
        ('firstname', 'First Name'),
        ('city', 'City'),
        ('zip', 'ZIP Code'),
    ]

    job = models.ForeignKey(SearchJob, related_name='tasks', on_delete=models.CASCADE)
    state_run = models.ForeignKey('StateRun', related_name='tasks', on_delete=models.CASCADE, null=True, blank=True)
    celery_task_id = models.CharField(max_length=255, blank=True, null=True)
    firstname = models.CharField(max_length=150, blank=True)
    lastname = models.CharField(max_length=150, blank=True)
    state = models.CharField(max_length=2, blank=True)
    city = models.CharField(max_length=150, blank=True)  # used by city-sweep axis
    axis = models.CharField(max_length=20, choices=AXIS_CHOICES, default='lastname')  # active expansion axis

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    error_message = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        if self.axis == 'city':
            return f"[CITY] {self.city} ({self.state}) - {self.status}"
        return f"{self.firstname} {self.lastname} ({self.state}) [{self.axis}] - {self.status}"


class PersonRecord(models.Model):
    task = models.ForeignKey(SearchTask, related_name='records', on_delete=models.CASCADE)

    firstname = models.CharField(max_length=150, blank=True, null=True)
    middlename = models.CharField(max_length=150, blank=True, null=True)
    lastname = models.CharField(max_length=150, blank=True, null=True)

    ssn = models.CharField(max_length=11, blank=True, null=True, unique=True)
    dob = models.CharField(max_length=20, blank=True, null=True)

    address = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=100, blank=True, null=True)
    state = models.CharField(max_length=2, blank=True, null=True)
    zip_code = models.CharField(max_length=20, blank=True, null=True)
    phone = models.CharField(max_length=20, blank=True, null=True)

    raw_data = models.JSONField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    def __str__(self):
        return f"{self.firstname} {self.lastname} - {self.address}"
