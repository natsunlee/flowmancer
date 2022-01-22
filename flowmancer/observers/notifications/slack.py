from ast import Not
import requests, json
from typing import Dict
from .notification import Notification

class SlackNotification(Notification):

    def __init__(self, **kwargs: Dict[str, str]) -> None:
        self._webhook = kwargs["webhook"]

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