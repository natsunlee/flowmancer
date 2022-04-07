import json

import requests

from .notification import Notification


class SlackNotification(Notification):
    def __init__(self, **kwargs: str) -> None:
        self._webhook = kwargs.pop("webhook")
        super().__init__(**kwargs)

    def send_notification(self, title: str, msg: str) -> None:
        requests.post(
            self._webhook,
            data=json.dumps({"text": title, "attachments": [{"text": msg}]}),
            headers={'Content-Type': 'application/json'},
        )
