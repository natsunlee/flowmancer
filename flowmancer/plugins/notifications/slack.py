import json

import requests

from ..plugin import plugin
from .notification import Notification


@plugin
class SlackWebhookNotification(Notification):
    webhook: str

    def send_notification(self, title: str, msg: str) -> None:
        requests.post(
            self.webhook,
            data=json.dumps({'text': title, 'attachments': [{'text': msg}]}),
            headers={'Content-Type': 'application/json'},
        )
