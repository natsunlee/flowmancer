from ast import Not
import requests, json
from .notification import Notification

class SlackNotification(Notification):

    def __init__(self, webhook: str) -> None:
        self._webhook = webhook
        self._checkpoint = 0

    def send_notification(self, title: str, msg: str) -> None:
        requests.post(
            self._webhook,
            data = json.dumps({
                "text": title,
                "attachments": [
                    {
                        "text": msg
                    }
                ]
            }),
            headers = {'Content-Type': 'application/json'}
        )