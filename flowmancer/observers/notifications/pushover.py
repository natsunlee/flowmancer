import http, urllib
from typing import Dict
from .notification import Notification

class PushoverNotification(Notification):

    def __init__(self, **kwargs: Dict[str, str]) -> None:
        self._app_token = kwargs["app_token"]
        self._user_key = kwargs["user_key"]

    def send_notification(self, title: str, msg: str) -> None:
        conn = http.client.HTTPSConnection("api.pushover.net:443")
        conn.request("POST", "/1/messages.json",
          urllib.parse.urlencode({
            "token": self._app_token,
            "user": self._user_key,
            "title": title,
            "message": msg
          }), { "Content-type": "application/x-www-form-urlencoded" })
        conn.getresponse()