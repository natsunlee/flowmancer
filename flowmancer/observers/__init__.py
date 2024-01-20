# noqa: F401
# Ensure implementations are registered
from . import notifications, progressbar
from .observer import Observer, observer

__all__ = ['Observer', 'observer']
