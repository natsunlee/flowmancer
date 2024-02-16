import json

import requests

from ..extension import extension
from .notification import Notification


@extension
class SlackWebhookNotification(Notification):
    webhook: str

    async def send_notification(self, title: str, msg: str) -> None:
        requests.post(
            self.webhook,
            data=json.dumps({'text': title, 'attachments': [{'text': msg}]}),
            headers={'Content-Type': 'application/json'},
        )
