import http, urllib
from .notification import Notification

class PushoverNotification(Notification):

    def __init__(self, app_token: str, user_key: str) -> None:
        self._app_token = app_token
        self._user_key = user_key
        self._checkpoint = 0

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