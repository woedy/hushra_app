from django.contrib import admin
from .models import HushraCredentials, SearchJob, SearchTask, PersonRecord, Proxy, StateRun


@admin.register(HushraCredentials)
class HushraCredentialsAdmin(admin.ModelAdmin):
    list_display = ('uuid', 'is_active', 'request_count', 'rate_limit_reset_time', 'updated_at')
    list_filter = ('is_active',)
    search_fields = ('uuid',)
    actions = ['reset_credentials']

    @admin.action(description='Reset selected credentials to active')
    def reset_credentials(self, request, queryset):
        for cred in queryset:
            cred.reset()
        self.message_user(request, f"Reset {queryset.count()} credential(s).")


@admin.register(Proxy)
class ProxyAdmin(admin.ModelAdmin):
    list_display = ('url', 'is_active', 'fail_count', 'last_used', 'created_at')
    list_filter = ('is_active',)
    search_fields = ('url',)


@admin.register(SearchJob)
class SearchJobAdmin(admin.ModelAdmin):
    list_display = ('name', 'created_at')
    search_fields = ('name',)


@admin.register(SearchTask)
class SearchTaskAdmin(admin.ModelAdmin):
    list_display = ('firstname', 'lastname', 'state', 'status', 'job', 'created_at')
    list_filter = ('status', 'state')
    search_fields = ('firstname', 'lastname', 'job__name')


@admin.register(PersonRecord)
class PersonRecordAdmin(admin.ModelAdmin):
    list_display = ('firstname', 'lastname', 'ssn', 'city', 'state', 'task', 'created_at')
    list_filter = ('state',)
    search_fields = ('firstname', 'lastname', 'ssn', 'address', 'city')
