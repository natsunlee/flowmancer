import requests

from ..extension import extension
from .notification import Notification


@extension
class PushoverNotification(Notification):
    app_token: str
    user_key: str

    async def send_notification(self, title: str, msg: str) -> None:
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        data = {'token': self.app_token, 'user': self.user_key, 'title': title, 'message': msg}
        requests.post('https://api.pushover.net/1/messages.json', headers=headers, data=data)
