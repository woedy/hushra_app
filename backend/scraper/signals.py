import json
from django.db.models.signals import post_save
from django.dispatch import receiver
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from .models import PersonRecord
from .serializers import PersonRecordSerializer

@receiver(post_save, sender=PersonRecord)
def broadcast_new_record(sender, instance, created, **kwargs):
    if created:
        channel_layer = get_channel_layer()
        serializer = PersonRecordSerializer(instance)
        
        async_to_sync(channel_layer.group_send)(
            "live_records",
            {
                "type": "record_scraped",
                "message": serializer.data
            }
        )
