import smtplib
from email.message import EmailMessage

from ..extension import extension
from .notification import Notification


@extension
class EmailNotification(Notification):
    recipient: str
    smtp_host: str
    smtp_port: int
    sender_user: str
    sender_host: str

    async def send_notification(self, title: str, msg: str) -> None:
        em = EmailMessage()
        em['From'] = f'{self.sender_user}@{self.sender_host}'
        em['Subject'] = title
        em['To'] = self.recipient
        em.set_content(msg)
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as s:
            s.send_message(em)
