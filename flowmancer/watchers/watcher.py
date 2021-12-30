import asyncio
from abc import ABC, abstractmethod
from typing import Dict
from ..executor import Executor
from flowmancer.jobspec.schema.v0_1 import JobDefinition

_root_event = asyncio.Event()

class Watcher(ABC):
    def __init__(self, **kwargs) -> None:
        self._event = asyncio.Event()
        self._sleep_time = 0.5
        self.executors: Dict[str, Executor] = kwargs["executors"]
        self.jobdef: JobDefinition = kwargs["jobdef"]

    @property
    def stop(self) -> bool:
        return _root_event.is_set()

    async def start_wrapper(self) -> None:
        await self.start()
        self._event.set()
    
    async def sleep(self, seconds: int = -1) -> None:
        if seconds < 0:
            seconds = self._sleep_time
        await asyncio.sleep(seconds)

    @abstractmethod
    async def start(self) -> None:
        pass