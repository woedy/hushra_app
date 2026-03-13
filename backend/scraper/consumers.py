import json
from channels.generic.websocket import AsyncWebsocketConsumer

class JobProgressConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.group_name = "live_records"
        
        # Join the broadcast group
        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        # Leave the group
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

    # Receive message from room group
    async def record_scraped(self, event):
        message = event['message']

        # Send message to WebSocket
        await self.send(text_data=json.dumps({
            'type': 'new_record',
            'data': message
        }))
