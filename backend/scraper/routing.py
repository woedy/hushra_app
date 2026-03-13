from django.urls import re_path
from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/records/$', consumers.JobProgressConsumer.as_asgi()),
]
