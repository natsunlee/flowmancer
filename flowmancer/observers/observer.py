import asyncio
from abc import ABC, abstractmethod
from ..managers.executormanager import ExecutorManager
from ..typedefs.enums import ExecutionState
from ..lifecycle import Lifecycle

class Observer(ABC, Lifecycle):

    def __init__(self,
        *,
        root_event: asyncio.Event,
        executors: ExecutorManager,
        sleep_time: float,
        restart: bool
    ) -> None:
        self._root_event = root_event
        self.executors = executors
        self.sleep_time = sleep_time
        self.restart = restart

    async def start(self, sleep_seconds: int = -1) -> None:
        try:
            if sleep_seconds <= 0:
                sleep_seconds = self.sleep_time

            self.on_create()

            if self.restart:
                self.on_restart()

            while not self._root_event.is_set():
                self.update()
                await asyncio.sleep(sleep_seconds)

            # Final update to allow observers to update once upon full completion.
            self.update()

            if self.executors.num_executors_in_state(
                ExecutionState.FAILED,
                ExecutionState.DEFAULTED
            ):
                self.on_failure()
            else:
                self.on_success()

            self.on_destroy()
        except asyncio.CancelledError:
            self.on_abort()

    @abstractmethod
    def update(self) -> None:
        pass