import smtplib, socket, getpass
from email.message import EmailMessage
from .notification import Notification

class EmailNotification(Notification):

    def __init__(
        self,
        recipient: str,
        smtp_host: str = "localhost",
        smtp_port: int = 0,
        sender_user: str = getpass.getuser(),
        sender_host: str = socket.gethostname()
    ) -> None:
        self._recipient = recipient
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._sender_user = sender_user
        self._sender_host = sender_host
        self._checkpoint = 0
    
    def send_notification(self, title: str, msg: str) -> None:
        em = EmailMessage()
        em["From"] = f"{self._sender_user}@{self._sender_host}"
        em["Subject"] = title
        em["To"] = self._recipient
        em.set_content(msg)
        with smtplib.SMTP(self._smtp_host, self._smtp_port) as s:
            s.send_message(em)