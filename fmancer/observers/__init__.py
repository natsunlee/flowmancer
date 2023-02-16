from abc import ABC, abstractmethod

from ..events import SerializableEvent
from ..lifecycle import AsyncLifecycle


class Observer(ABC, AsyncLifecycle):
    @abstractmethod
    async def update(self, e: SerializableEvent) -> None:
        pass
