import asyncio
from abc import ABC, abstractmethod
from typing import Dict
from ..executor import Executor

_root_event = asyncio.Event()

class Watcher(ABC):
    def __init__(self, executors: Dict[str, Executor]) -> None:
        self._event = asyncio.Event()
        self.executors = executors

    @property
    def stop(self) -> bool:
        return _root_event.is_set()

    async def start_wrapper(self) -> None:
        await self.start()
        self._event.set()

    @abstractmethod
    async def start(self) -> None:
        pass