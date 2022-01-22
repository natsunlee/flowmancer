import smtplib
from typing import Dict
from email.message import EmailMessage
from .notification import Notification

class EmailNotification(Notification):

    def __init__(self, **kwargs: Dict[str, str]) -> None:
        self._recipient = kwargs["recipient"]
        self._smtp_host = kwargs["smtp_host"]
        self._smtp_port = kwargs["smtp_port"]
        self._sender_user = kwargs["sender_user"]
        self._sender_host = kwargs["sender_host"]
    
    def send_notification(self, title: str, msg: str) -> None:
        em = EmailMessage()
        em["From"] = f"{self._sender_user}@{self._sender_host}"
        em["Subject"] = title
        em["To"] = self._recipient
        em.set_content(msg)
        with smtplib.SMTP(self._smtp_host, self._smtp_port) as s:
            s.send_message(em)