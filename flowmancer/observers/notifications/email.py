import smtplib
from email.message import EmailMessage
from typing import Union

from .notification import Notification


class EmailNotification(Notification):
    def __init__(self, **kwargs: Union[str, int]) -> None:
        self._recipient = str(kwargs.pop("recipient"))
        self._smtp_host = str(kwargs.pop("smtp_host"))
        self._smtp_port = int(kwargs.pop("smtp_port"))
        self._sender_user = str(kwargs.pop("sender_user"))
        self._sender_host = str(kwargs.pop("sender_host"))
        super().__init__(**kwargs)

    def send_notification(self, title: str, msg: str) -> None:
        em = EmailMessage()
        em["From"] = f"{self._sender_user}@{self._sender_host}"
        em["Subject"] = title
        em["To"] = self._recipient
        em.set_content(msg)
        with smtplib.SMTP(self._smtp_host, self._smtp_port) as s:
            s.send_message(em)
