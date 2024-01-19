import json
from typing import Dict

import requests

from .. import observer
from . import Notification


@observer
class SlackNotification(Notification):
    def __init__(self, **kwargs: Dict[str, str]) -> None:
        self._webhook = kwargs.pop('webhook')
        super().__init__(**kwargs)

    def send_notification(self, title: str, msg: str) -> None:
        requests.post(
            self._webhook,  # type: ignore
            data=json.dumps({'text': title, 'attachments': [{'text': msg}]}),
            headers={'Content-Type': 'application/json'},
        )
