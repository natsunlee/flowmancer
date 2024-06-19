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
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, TextIO, Type, Union, cast

from .eventbus import EventBus
from .eventbus.execution import ExecutionState, ExecutionStateTransition, SerializableExecutionEvent
from .eventbus.log import (
    LogWriter,
    SerializableLogEvent,
    StdErrLogWriterWrapper,
    StdOutLogWriterWrapper,
    TaskLogWriterWrapper,
)
from .exceptions import TaskClassNotFoundError
from .task import Task, _task_classes


async def _default_await_dependencies() -> bool:
    return True


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
    task_class: Type[Task],
    parameters: Optional[Dict[str, Any]],
    log_event_bus: Optional[EventBus[SerializableLogEvent]],
    result: ProcessResult,
    shared_dict: Optional[Union[Dict[str, Any], DictProxy[str, Any]]] = None,
    is_restart: bool = False,
    depends_on: Optional[List[str]] = None
):
    base_log_writer = LogWriter(task_name, log_event_bus)
    # Pydantic's BaseModel appears to interfere with the Manager objects when it serializes model values...
    # As a result, any Manager objects should be assigned here directly after being split off into a new process.
    parameters = parameters or dict()
    parameters['logger'] = TaskLogWriterWrapper(base_log_writer)
    parameters['shared_dict'] = cast(Dict[str, Any], shared_dict) if shared_dict is not None else dict()
    parameters['metadata'] = {
        'name': task_name,
        'variant': task_class.__name__,
        'depends_on': depends_on or []
    }
    task_instance = task_class(**parameters)

    # Bind signal only in new child process
    stdout_log_writer = cast(TextIO, StdOutLogWriterWrapper(base_log_writer))
    stderr_log_writer = cast(TextIO, StdErrLogWriterWrapper(base_log_writer))

    def _exec_lifecycle_stage(stage: Callable[[], None]) -> None:
        try:
            stage()
        except Exception:
            print(traceback.format_exc(), file=stderr_log_writer)
            result.is_failed = True

    signal.signal(signal.SIGTERM, lambda *_: _exec_lifecycle_stage(task_instance.on_abort))
    _sout = sys.stdout
    _serr = sys.stderr

    try:
        sys.stdout = stdout_log_writer
        sys.stderr = stderr_log_writer

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
        print(traceback.format_exc(), file=sys.stderr)
        result.is_failed = True
    finally:
        _exec_lifecycle_stage(task_instance.on_destroy)
        base_log_writer.close()
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
        parameters: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None
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
        self.depends_on = depends_on

    @property
    def state(self) -> ExecutionState:
        return self._state

    @state.setter
    def state(self, val: ExecutionState) -> None:
        event = ExecutionStateTransition(name=self.name, from_state=self._state, to_state=val)
        if self.execution_event_bus is not None:
            self.execution_event_bus.put(event)
        self._state = val

    def get_task_class(self) -> Type[Task]:
        if inspect.isclass(self.task_class) and issubclass(self.task_class, Task):
            return self.task_class
        elif type(self.task_class) == str:
            if self.task_class not in _task_classes:
                raise TaskClassNotFoundError(self.task_class)
            return _task_classes[self.task_class]
        else:
            raise TypeError('The `task_class` param must be either an extension of `Task` or the string name of one.')

    def get_task_instance(self) -> Task:
        return self.get_task_class()(**(self.parameters or {}))

    @asynccontextmanager
    async def acquire_lock(self) -> AsyncIterator[Any]:
        try:
            yield await self.semaphore.acquire() if self.semaphore else None
        finally:
            if self.semaphore:
                self.semaphore.release()

    # Quick patch until the execution handling is improved...this is to ensure all Event objects are init'ed before
    # firing up Executors to avoid situations where `self.event` for a dependency is None/not yet initialized.
    def init_event(self) -> None:
        # Need to wait for main loop to initiate before spawning Event
        self.event = asyncio.Event()

    async def wait(self) -> None:
        await self.event.wait()

    async def start(self) -> None:
        try:
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
                            self.get_task_class(),
                            self.parameters,
                            self.log_event_bus,
                            result,
                            self.shared_dict,
                            self.is_restart,
                            self.depends_on
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
