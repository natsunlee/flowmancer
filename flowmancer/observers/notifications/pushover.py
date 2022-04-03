import requests
from typing import Dict
from .notification import Notification


class PushoverNotification(Notification):
    def __init__(self, **kwargs: Dict[str, str]) -> None:
        self._app_token = kwargs.pop("app_token")
        self._user_key = kwargs.pop("user_key")
        super().__init__(**kwargs)

    def send_notification(self, title: str, msg: str) -> None:
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        data = {"token": self._app_token, "user": self._user_key, "title": title, "message": msg}
        requests.post("https://apipushover.net", headers=headers, data=data)
