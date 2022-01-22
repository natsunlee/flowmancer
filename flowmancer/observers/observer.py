import asyncio
from abc import ABC, abstractmethod
from typing import Dict
from ..managers.executormanager import ExecutorManager
from ..typedefs.enums import ExecutionState
from ..executors.executor import Executor
from ..typedefs.models import JobDefinition
from ..lifecycle import Lifecycle

class Observer(ABC, Lifecycle):

    _root_event = asyncio.Event()
    executors: ExecutorManager
    restart = False
    sleep_time = 0.5

    # Required Observers
    @classmethod
    async def init_synchro(cls) -> None:
        pending = set(cls.executors.values())
        while pending:
            for ex in pending.copy():
                if not ex.is_alive:
                    pending.remove(ex)
            await asyncio.sleep(cls.sleep_time)
        cls._root_event.set()

    @property
    def executors(self) -> Dict[str, Executor]:
        return self.__class__.executors
    @property
    def jobdef(self) -> JobDefinition:
        return self.__class__.jobdef
    @property
    def is_restart(self) -> bool:
        return self.__class__.restart

    async def start(self, sleep_seconds: int = -1) -> None:
        if sleep_seconds <= 0:
            sleep_seconds = self.__class__.sleep_time

        self.on_create()

        if self.is_restart:
            self.on_restart()
        
        while not self.__class__._root_event.is_set():
            self.update()
            await asyncio.sleep(sleep_seconds)
        
        if self.executors.num_executors_in_state(
            ExecutionState.FAILED,
            ExecutionState.DEFAULTED
        ):
            self.on_failure()
        else:
            self.on_success()
        
        self.on_destroy()

    @abstractmethod
    def update(self) -> None:
        pass