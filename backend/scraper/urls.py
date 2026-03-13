from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    HushraCredentialsViewSet, SearchJobViewSet, SearchTaskViewSet,
    PersonRecordViewSet, ProxyViewSet, StatsView, ExportCSVView,
    GlobalSettingViewSet
)

router = DefaultRouter()
router.register(r'credentials', HushraCredentialsViewSet)
router.register(r'proxies', ProxyViewSet)
router.register(r'jobs', SearchJobViewSet)
router.register(r'tasks', SearchTaskViewSet)
router.register(r'records', PersonRecordViewSet)
router.register(r'settings', GlobalSettingViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('stats/', StatsView.as_view(), name='stats'),
    path('export/', ExportCSVView.as_view(), name='export-csv'),
]
