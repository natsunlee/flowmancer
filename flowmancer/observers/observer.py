import asyncio
from abc import ABC, abstractmethod
from typing import Dict
from ..typedefs.enums import ExecutionState
from ..executor import Executor
from flowmancer.jobspec.schema.v0_1 import JobDefinition

_root_event = asyncio.Event()

class Observer(ABC):
    def __init__(self, **kwargs) -> None:
        self._event = asyncio.Event()
        self._sleep_time = kwargs.get("sleep_time", 0.5)
        self.executors: Dict[str, Executor] = kwargs["executors"]
        self.jobdef: JobDefinition = kwargs["jobdef"]

    async def sleep(self, seconds: int = -1) -> None:
        if seconds < 0:
            seconds = self._sleep_time
        await asyncio.sleep(seconds)

    async def start(self) -> None:
        while not _root_event.is_set():
            self.update()
            await asyncio.sleep(self._sleep_time)
        self._event.set()
        self.on_destroy()
    
    def failed_executors_exist(self) -> bool:
        for ex in self.executors.values():
            if ex.state == ExecutionState.FAILED:
                return True
        return False

    @abstractmethod
    def update(self) -> None:
        pass

    def on_destroy(self) -> None:
        # Optional cleanup method.
        pass