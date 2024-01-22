from __future__ import annotations

import asyncio
import inspect
import signal
import sys
import traceback
from contextlib import asynccontextmanager
from multiprocessing import Process
from multiprocessing.managers import DictProxy
from multiprocessing.sharedctypes import Value
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, Optional, Set, TextIO, Type, Union, cast

from .eventbus import EventBus
from .eventbus.execution import ExecutionState, ExecutionStateTransition, SerializableExecutionEvent
from .eventbus.log import LogWriter, SerializableLogEvent
from .task import Task, _task_classes


async def _default_await_dependencies() -> bool:
    return True


class ExecutionStateMap:
    def __init__(self) -> None:
        self.data: Dict[ExecutionState, Set[str]] = dict()

    def __getitem__(self, k: Union[str, ExecutionState]) -> Set[str]:
        es = ExecutionState(k)
        if es not in self.data:
            self.data[es] = set()
        return self.data[ExecutionState(k)]

    def __str__(self):
        return str(self.data)

    def items(self):
        return self.data.items()

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()


class ProcessResult:
    def __init__(self) -> None:
        self._retcode = Value('i', 0)

    @property
    def is_failed(self) -> bool:
        return bool(self._retcode.value)  # type: ignore

    @is_failed.setter
    def is_failed(self, v: bool) -> None:
        self._retcode.value = v  # type: ignore


def exec_task_lifecycle(
    task_name: str,
    task_instance: Task,
    log_event_bus: Optional[EventBus[SerializableLogEvent]],
    result: ProcessResult,
    shared_dict: Optional[Union[Dict[str, Any], DictProxy[str, Any]]] = None,
    is_restart: bool = False
):
    # Pydantic's BaseModel appears to interfere with the Manager objects when it serializes model values...
    # As a result, any Manager objects should be assigned here directly after being split off into a new process.
    if shared_dict is not None:
        task_instance._shared_dict = cast(Dict[str, Any], shared_dict)

    def _exec_lifecycle_stage(stage: Callable[[], None]) -> None:
        try:
            stage()
        except Exception:
            print(traceback.format_exc())
            result.is_failed = True

    # Bind signal only in new child process
    signal.signal(signal.SIGTERM, lambda *_: _exec_lifecycle_stage(task_instance.on_abort))
    writer = cast(TextIO, LogWriter(task_name, log_event_bus))

    _sout = sys.stdout
    _serr = sys.stderr

    try:
        sys.stdout = writer
        sys.stderr = writer

        _exec_lifecycle_stage(task_instance.on_create)

        if is_restart:
            _exec_lifecycle_stage(task_instance.on_restart)

        _exec_lifecycle_stage(task_instance.run)

        if result.is_failed:
            _exec_lifecycle_stage(task_instance.on_failure)
        else:
            _exec_lifecycle_stage(task_instance.on_success)
            result.is_failed = False
    except Exception:
        print(traceback.format_exc())
        result.is_failed = True
    finally:
        _exec_lifecycle_stage(task_instance.on_destroy)
        writer.close()
        sys.stdout = _sout
        sys.stderr = _serr


class Executor:
    def __init__(
        self,
        name: str,
        task_class: Union[str, Type[Task]],
        log_event_bus: Optional[EventBus[SerializableLogEvent]] = None,
        execution_event_bus: Optional[EventBus[SerializableExecutionEvent]] = None,
        shared_dict: Optional[Union[DictProxy[str, Any], Dict[str, Any]]] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
        max_attempts: int = 1,
        backoff: int = 0,
        await_dependencies: Callable[[], Coroutine[Any, Any, bool]] = _default_await_dependencies,
        is_restart: bool = False,
        parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        self.name = name
        self.log_event_bus = log_event_bus
        self.execution_event_bus = execution_event_bus
        self.shared_dict = shared_dict
        self.max_attempts = max_attempts
        self.semaphore = semaphore
        self.backoff = backoff
        self.task_class = task_class
        self.parameters = parameters
        self.await_dependencies = await_dependencies
        self._state = ExecutionState.INIT
        self.proc: Optional[Process] = None
        self.is_restart = is_restart

    @property
    def state(self) -> ExecutionState:
        return self._state

    @state.setter
    def state(self, val: ExecutionState) -> None:
        event = ExecutionStateTransition(name=self.name, from_state=self._state, to_state=val)
        if self.execution_event_bus is not None:
            self.execution_event_bus.put(event)
        self._state = val

    def get_task_instance(self) -> Task:
        parameters = self.parameters or dict()
        if inspect.isclass(self.task_class) and issubclass(self.task_class, Task):
            return self.task_class(**parameters)
        elif type(self.task_class) == str:
            return _task_classes[self.task_class](**parameters)
        else:
            raise TypeError('The `task_class` param must be either an extension of `Task` or the string name of one.')

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

            # In the event of a restart and this task is already complete, return immediately.
            if self.state == ExecutionState.COMPLETED:
                self.event.set()
                return

            # Trigger a state change from INIT -> PENDING
            self.state = ExecutionState.PENDING

            if not await self.await_dependencies():
                self.state = ExecutionState.DEFAULTED
                self.event.set()
                return

            # In the event of skipped task, return immediately.
            if self.state == ExecutionState.SKIP:
                self.event.set()
                return

            attempts = 0
            result = ProcessResult()
            while attempts < self.max_attempts and self.state == ExecutionState.PENDING:
                async with self.acquire_lock():
                    result.is_failed = False
                    self.state = ExecutionState.RUNNING
                    attempts += 1
                    self.proc = Process(
                        target=exec_task_lifecycle,
                        args=(
                            self.name,
                            self.get_task_instance(),
                            self.log_event_bus,
                            result,
                            self.shared_dict,
                            self.is_restart
                        ),
                        daemon=False
                    )
                    self.proc.start()
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self.proc.join)

                # Restart check
                if result.is_failed and (attempts < self.max_attempts):
                    self.state = ExecutionState.PENDING
                    await asyncio.sleep(self.backoff)

            if result.is_failed:
                self.state = ExecutionState.FAILED
            else:
                self.state = ExecutionState.COMPLETED
        except asyncio.CancelledError:
            self.terminate()
            self.state = ExecutionState.ABORTED
        finally:
            self.event.set()

    def terminate(self) -> None:
        if self.proc is not None:
            # Send SIGTERM to child process
            self.proc.terminate()
