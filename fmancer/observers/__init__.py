from abc import ABC, abstractmethod

from ..executor import SerializableExecutionEvent
from ..lifecycle import AsyncLifecycle


class Observer(ABC, AsyncLifecycle):
    @abstractmethod
    async def update(self, e: SerializableExecutionEvent) -> None:
        pass
