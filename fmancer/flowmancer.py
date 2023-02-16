import asyncio
import contextlib
from collections import namedtuple
from multiprocessing import Manager
from multiprocessing.managers import DictProxy
from queue import Queue
from typing import Any, Dict, List, Optional, Type, Union

from .containers import States
from .enums import ExecutionState
from .events import ExecutionStateTransition, SerializableEvent
from .executor import Executor
from .loggers import Logger
from .loggers.file import FileLogger
from .loggers.messages import SerializableMessage
from .observers import Observer
from .observers.progressbar import RichProgressBar
from .task import Task

ExecutorDetails = namedtuple('ExecutorDetails', 'instance dependencies')


@contextlib.contextmanager
def create_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.stop()
    loop.close()


class Flowmancer:
    def __init__(self) -> None:
        manager = Manager()
        self._log_queue: Queue[Any] = manager.Queue()
        self._event_queue: Queue[Any] = manager.Queue()
        self._shared_dict: DictProxy[Any, Any] = manager.dict()
        self._executors: Dict[str, ExecutorDetails] = dict()
        self._states = States()
        self._observer_interval_seconds = 0.25
        self._registered_observers: List[Observer] = []
        self._registered_loggers: List[Logger] = []

    def start(self) -> int:
        return asyncio.run(self._initiate())

    async def _initiate(self) -> int:
        with create_loop():
            root_event = asyncio.Event()
            observer_tasks = self._init_observers(root_event)
            executor_tasks = self._init_executors(root_event)
            logger_tasks = self._init_loggers(root_event)
            await asyncio.gather(*observer_tasks, *executor_tasks, *logger_tasks)
        return len(self._states[ExecutionState.FAILED]) + len(self._states[ExecutionState.DEFAULTED])

    # ASYNC INITIALIZATIONS
    def _init_executors(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        async def _synchro() -> None:
            while not root_event.is_set():
                if (
                    not self._states[ExecutionState.INIT]
                    and not self._states[ExecutionState.PENDING]
                    and not self._states[ExecutionState.RUNNING]
                ):
                    root_event.set()
                else:
                    await asyncio.sleep(self._observer_interval_seconds)

        return [asyncio.create_task(_synchro())] + [
            asyncio.create_task(dtl.instance.start()) for dtl in self._executors.values()
        ]

    def _init_loggers(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        self._registered_loggers = [FileLogger()]

        async def _pusher() -> None:
            while True:
                while not self._log_queue.empty():
                    m = SerializableMessage.deserialize(self._log_queue.get())
                    for log in self._registered_loggers:
                        await log.update(m)
                if root_event.is_set():
                    for log in self._registered_loggers:
                        await log.on_destroy()
                    break
                await asyncio.sleep(self._observer_interval_seconds)

        return [asyncio.create_task(_pusher())]

    def _init_observers(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        self._registered_observers = [RichProgressBar()]

        async def _pusher() -> None:
            for obs in self._registered_observers:
                await obs.on_create()
            while True:
                while not self._event_queue.empty():
                    e = SerializableEvent.deserialize(self._event_queue.get())
                    if isinstance(e, ExecutionStateTransition):
                        self._states[e.from_state].remove(e.name)
                        self._states[e.to_state].add(e.name)
                    for obs in self._registered_observers:
                        await obs.update(e)
                if root_event.is_set():
                    for obs in self._registered_observers:
                        await obs.on_destroy()
                    break
                await asyncio.sleep(self._observer_interval_seconds)

        return [asyncio.create_task(_pusher())]

    # EXECUTORS
    def _dependencies_are_valid(self) -> bool:
        for name, dtl in self._executors.items():
            for dep in dtl.dependencies:
                # Missing dependency
                if dep not in self._executors.keys():
                    return False
                # Self referencing dependency
                if dep == name:
                    return False
        return True

    def add_executor(self, name: str, task_class: Union[str, Type[Task]], deps: Optional[List[str]] = None) -> None:
        async def await_dependencies() -> bool:
            for dep_name in self._executors[name].dependencies:
                d = self._executors[dep_name].instance
                await d.wait()
                if d.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED, ExecutionState.ABORTED):
                    return False
            return True

        e = Executor(
            name=name,
            task_class=task_class,
            log_queue=self._log_queue,
            event_queue=self._event_queue,
            await_dependencies=await_dependencies,
        )

        self._executors[name] = ExecutorDetails(instance=e, dependencies=(deps or []))
        self._states[ExecutionState.INIT].add(name)
