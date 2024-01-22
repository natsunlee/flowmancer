# noqa: F401
# Ensure implementations are registered
from . import email, pushover, slack
from .notification import Notification

__all__ = ['Notification']
