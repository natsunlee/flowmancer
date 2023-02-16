from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager
from enum import Enum
from multiprocessing import Process
from queue import Queue
from typing import (Any, AsyncIterator, Callable, Coroutine, Dict, Optional,
                    Type, Union, cast)

from pydantic import BaseModel

from .task import Task, _task_classes

_event_classes = dict()


def event(t: type[SerializableExecutionEvent]) -> Any:
    if not issubclass(t, SerializableExecutionEvent):
        raise TypeError(f'Must extend `SerializableExecutionEvent` type: {t.__name__}')
    _event_classes[t.__name__] = t
    return t


class ExecutionState(Enum):
    FAILED = "F"
    PENDING = "P"
    RUNNING = "R"
    DEFAULTED = "D"
    COMPLETED = "C"
    ABORTED = "A"
    SKIP = "S"
    INIT = "_"


class SerializableExecutionEvent(BaseModel):
    class Config:
        use_enum_values = True

    def serialize(self) -> Dict[str, Any]:
        return {"event": type(self).__name__, "body": self.dict()}

    @classmethod
    def deserialize(cls, e: Dict[str, Any]) -> Any:
        if "event" not in e or "body" not in e or not isinstance(e, dict):
            return UnknownExecutionEvent(content=str(e))
        else:
            return _event_classes[e["event"]](**e["body"])


@event
class ExecutionStateTransition(SerializableExecutionEvent):
    name: str
    from_state: ExecutionState
    to_state: ExecutionState


@event
class UnknownExecutionEvent(SerializableExecutionEvent):
    content: str


class Executor:
    __slots__ = (
        "event",
        "proc",
        "task_instance",
        "semaphore",
        "await_dependencies",
        "_state",
        "max_attempts",
        "backoff",
        "event_queue",
        "name",
    )

    def __init__(
        self,
        name: str,
        task_class: Union[str, Type[Task]],
        log_queue: Optional[Queue[Any]] = None,
        event_queue: Optional[Queue[Any]] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
        max_attempts: int = 1,
        backoff: int = 0,
        await_dependencies: Optional[Callable[[], Coroutine[Any, Any, bool]]] = None,
    ) -> None:
        if inspect.isclass(task_class) and issubclass(task_class, Task):
            self.task_instance = task_class(name, log_queue)
        else:
            self.task_instance = _task_classes[cast(str, task_class)](name, log_queue)
        self.name = name
        self.max_attempts = max_attempts
        self.semaphore = semaphore
        self._state = ExecutionState.INIT
        self.backoff = backoff
        self.event_queue = event_queue

        async def _default_await_dependencies() -> bool:
            return True

        self.await_dependencies = await_dependencies or _default_await_dependencies

    @property
    def state(self) -> ExecutionState:
        return self._state

    @state.setter
    def state(self, val: ExecutionState) -> None:
        event = ExecutionStateTransition(name=self.name, from_state=self._state, to_state=val)
        if self.event_queue is not None:
            self.event_queue.put(event.serialize())
        self._state = val

    @asynccontextmanager
    async def acquire_lock(self) -> AsyncIterator[Any]:
        try:
            yield await self.semaphore.acquire() if self.semaphore else None
        finally:
            if self.semaphore:
                self.semaphore.release()

    async def wait(self) -> None:
        await self.event.wait()

    async def start(self) -> None:
        try:
            # Need to wait for main loop to initiate before spawning Event
            self.event = asyncio.Event()

            # Trigger a state change from NONE -> PENDING
            self.state = ExecutionState.PENDING

            # In the event of a restart and this task is already complete, return immediately.
            if self.state == ExecutionState.COMPLETED:
                self.event.set()
                return

            if not await self.await_dependencies():
                self.state = ExecutionState.DEFAULTED
                self.event.set()
                return

            # In the event of skipped task, return immediately.
            if self.state == ExecutionState.SKIP:
                self.event.set()
                return

            attempts = 0
            while attempts < self.max_attempts and self.state == ExecutionState.PENDING:
                async with self.acquire_lock():
                    self.state = ExecutionState.RUNNING
                    attempts += 1
                    self.proc = Process(target=self.task_instance.run_lifecycle, daemon=False)
                    self.proc.start()
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self.proc.join)

                # Restart check
                if self.task_instance.is_failed and (attempts < self.max_attempts):
                    self.state = ExecutionState.PENDING
                    await asyncio.sleep(self.backoff)

            self.state = ExecutionState.FAILED if self.task_instance.is_failed else ExecutionState.COMPLETED
        except asyncio.CancelledError:
            self.terminate()
            self.state = ExecutionState.ABORTED
        finally:
            self.event.set()

    def terminate(self) -> None:
        if self.proc is not None:
            # Send SIGTERM to child process
            self.proc.terminate()
