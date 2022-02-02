import smtplib
from typing import Dict
from email.message import EmailMessage
from .notification import Notification

class EmailNotification(Notification):

    def __init__(self, **kwargs: Dict[str, str]) -> None:
        self._recipient = kwargs.pop("recipient")
        self._smtp_host = kwargs.pop("smtp_host")
        self._smtp_port = kwargs.pop("smtp_port")
        self._sender_user = kwargs.pop("sender_user")
        self._sender_host = kwargs.pop("sender_host")
        super().__init__(**kwargs)
    
    def send_notification(self, title: str, msg: str) -> None:
        em = EmailMessage()
        em["From"] = f"{self._sender_user}@{self._sender_host}"
        em["Subject"] = title
        em["To"] = self._recipient
        em.set_content(msg)
        with smtplib.SMTP(self._smtp_host, self._smtp_port) as s:
            s.send_message(em)