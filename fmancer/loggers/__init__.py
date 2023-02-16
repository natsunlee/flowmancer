from abc import ABC, abstractmethod

from ..lifecycle import AsyncLifecycle
from .messages import LogMessage


class Logger(ABC, AsyncLifecycle):
    @abstractmethod
    async def update(self, m: LogMessage) -> None:
        pass
