from rest_framework import serializers
from .models import HushraCredentials, SearchJob, SearchTask, PersonRecord, Proxy, GlobalSetting, StateRun
import re


class GlobalSettingSerializer(serializers.ModelSerializer):
    value = serializers.SerializerMethodField()

    class Meta:
        model = GlobalSetting
        fields = ['id', 'key', 'value', 'updated_at']

    def get_value(self, obj):
        val = obj.value.lower()
        if val == 'true': return True
        if val == 'false': return False
        try:
            return int(val)
        except:
            return obj.value


class HushraCredentialsSerializer(serializers.ModelSerializer):
    class Meta:
        model = HushraCredentials
        fields = ['id', 'uuid', 'is_active', 'rate_limit_reset_time', 'request_count', 'updated_at']


class ProxySerializer(serializers.ModelSerializer):
    masked_url = serializers.SerializerMethodField()

    class Meta:
        model = Proxy
        fields = ['id', 'url', 'masked_url', 'is_active', 'fail_count', 'last_used', 'created_at']
        extra_kwargs = {
            'url': {'write_only': True}
        }

    def get_masked_url(self, obj):
        """Return the URL with any password replaced by *** for display."""
        return re.sub(r'(:)([^@/]+)(@)', r'\1***\3', obj.url)


class PersonRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = PersonRecord
        fields = '__all__'


class SearchTaskSerializer(serializers.ModelSerializer):
    records = PersonRecordSerializer(many=True, read_only=True)

    class Meta:
        model = SearchTask
        fields = ['id', 'job', 'celery_task_id', 'firstname', 'lastname', 'state', 'city', 'axis', 'status', 'error_message', 'created_at', 'updated_at', 'records']


class SearchJobSerializer(serializers.ModelSerializer):
    tasks_count = serializers.IntegerField(source='tasks.count', read_only=True)
    completed_tasks = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    class Meta:
        model = SearchJob
        fields = ['id', 'name', 'status', 'created_at', 'tasks_count', 'completed_tasks']

    def get_completed_tasks(self, obj):
        return obj.tasks.filter(status='COMPLETED').count()

    def get_status(self, obj):
        # 1. Manually stopped jobs stay STOPPED
        if obj.status == 'STOPPED':
            return 'STOPPED'
            
        total = obj.tasks.count()
        if total > 0:
            pending = obj.tasks.filter(status__in=['PENDING', 'IN_PROGRESS']).count()
            stopped = obj.tasks.filter(status='STOPPED').count()
            
            # If nothing is pending/running anymore
            if pending == 0:
                # But some tasks are explicitly STOPPED = The job is STOPPED
                if stopped > 0:
                    return 'STOPPED'
                # Otherwise, it truly finished
                return 'COMPLETED'
                
        return obj.status
