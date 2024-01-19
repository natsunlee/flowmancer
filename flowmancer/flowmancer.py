from __future__ import annotations

import asyncio
import contextlib
import time
from argparse import ArgumentParser
from collections import namedtuple
from multiprocessing import Manager
from multiprocessing.managers import DictProxy
from typing import Any, Dict, List, Optional, Type, Union

from .checkpoint import CheckpointContents, NoCheckpointAvailableError
from .checkpoint.file import FileCheckpoint
from .eventbus import EventBus
from .eventbus.execution import ExecutionState, ExecutionStateTransition, SerializableExecutionEvent
from .eventbus.log import SerializableLogEvent
from .executor import ExecutionStateMap, Executor
from .jobdefinition import file  # noqa: F401
from .jobdefinition import JobDefinition, TaskDefinition, _job_definition_classes
from .loggers import Logger
from .loggers.file import FileLogger
from .observers import Observer, _observer_classes
from .observers.notifications import pushover  # noqa: F401
from .observers.progressbar import RichProgressBar
from .task import Task

__all__ = ['Flowmancer']

ExecutorDetails = namedtuple('ExecutorDetails', 'instance dependencies')


# Need to explicitly manage loop in case multiple instances of Flowmancer are run.
@contextlib.contextmanager
def create_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.stop()
    loop.close()


class Flowmancer:
    def __init__(self, name: str = 'flow', test: bool = False, debug: bool = False) -> None:
        manager = Manager()
        self.name = name
        self.concurrency = 0
        self._test = test
        self._debug = debug
        self._log_event_bus = EventBus[SerializableLogEvent](manager.Queue())
        self._execution_event_bus = EventBus[SerializableExecutionEvent]()
        self._shared_dict: DictProxy[str, Any] = manager.dict()
        self._executors: Dict[str, ExecutorDetails] = dict()
        self._states = ExecutionStateMap()
        self._observer_interval_seconds = 0.25
        self._registered_observers: List[Observer] = [RichProgressBar()]
        self._registered_loggers: List[Logger] = []
        self._semaphore = asyncio.Semaphore(self.concurrency) if self.concurrency > 0 else None
        self._checkpoint = FileCheckpoint(checkpoint_name=self.name)

    def start(self) -> int:
        if not self._test:
            self._process_cmd_args()
        return asyncio.run(self._initiate())

    async def _initiate(self) -> int:
        with create_loop():
            root_event = asyncio.Event()
            observer_tasks = self._init_observers(root_event)
            executor_tasks = self._init_executors(root_event)
            logger_tasks = self._init_loggers(root_event)
            checkpoint_task = self._init_checkpointer(root_event)
            await asyncio.gather(*observer_tasks, *executor_tasks, *logger_tasks, checkpoint_task)
        return len(self._states[ExecutionState.FAILED]) + len(self._states[ExecutionState.DEFAULTED])

    def _process_cmd_args(self) -> None:
        parser = ArgumentParser(description='Flowmancer job execution options.')
        parser.add_argument('-j', '--jobdef', action='store', dest='jobdef')
        parser.add_argument('-r', '--restart', action='store_true', dest='restart', default=False)
        parser.add_argument('-d', '--debug', action='store_true', dest='debug', default=False)
        parser.add_argument('--skip', action='append', dest='skip', default=[])
        parser.add_argument('--run-to', action='store', dest='run_to')
        parser.add_argument('--run-from', action='store', dest='run_from')
        parser.add_argument('--max-parallel', action='store', type=int, dest='max_parallel')

        args = parser.parse_args()
        self._debug = args.debug

        if args.restart:
            try:
                cp = self._checkpoint.read_checkpoint()
                self.load_job_definition(cp.job_definition)
                self._shared_dict.update(cp.shared_dict)
                completed = cp.states[ExecutionState.COMPLETED].copy()
                cp.states[ExecutionState.INIT].update(cp.states[ExecutionState.FAILED])
                cp.states[ExecutionState.INIT].update(cp.states[ExecutionState.ABORTED])
                cp.states[ExecutionState.INIT].update(cp.states[ExecutionState.RUNNING])
                cp.states[ExecutionState.INIT].update(cp.states[ExecutionState.PENDING])
                cp.states[ExecutionState.INIT].update(cp.states[ExecutionState.DEFAULTED])
                cp.states[ExecutionState.INIT].update(cp.states[ExecutionState.COMPLETED])
                cp.states[ExecutionState.FAILED].clear()
                cp.states[ExecutionState.ABORTED].clear()
                cp.states[ExecutionState.RUNNING].clear()
                cp.states[ExecutionState.PENDING].clear()
                cp.states[ExecutionState.DEFAULTED].clear()
                cp.states[ExecutionState.COMPLETED].clear()
                self._states = cp.states
                for n in completed:
                    self._executors[n].instance.state = ExecutionState.COMPLETED
            except NoCheckpointAvailableError:
                print(f"No checkpoint file found for '{self.name}'. Starting new job.")
        elif args.jobdef:
            self.load_job_definition(args.jobdef)

    # ASYNC INITIALIZATIONS
    def _init_checkpointer(self, root_event) -> asyncio.Task:
        job_definition = self.get_job_definition()

        async def _write_checkpoint() -> None:
            last_write = 0
            while True:
                if (time.time() - last_write) >= 10:
                    self._checkpoint.write_checkpoint(
                        CheckpointContents(
                            name=self.name,
                            states=self._states,
                            job_definition=job_definition,
                            shared_dict=self._shared_dict.copy()
                        )
                    )
                if root_event.is_set():
                    if (
                        not self._states[ExecutionState.FAILED]
                        and not self._states[ExecutionState.DEFAULTED]
                        and not self._states[ExecutionState.ABORTED]
                    ):
                        self._checkpoint.clear_checkpoint()
                    break
                else:
                    await asyncio.sleep(self._observer_interval_seconds)

        return asyncio.create_task(_write_checkpoint())

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
            asyncio.create_task(dtl.instance.start())
            for name, dtl in self._executors.items()
            if name in self._states[ExecutionState.INIT]
        ]

    def _init_loggers(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        self._registered_loggers = [FileLogger()] if not self._test else []

        async def _pusher() -> None:
            for log in self._registered_loggers:
                await log.on_create()
            while True:
                while not self._log_event_bus.empty():
                    m = self._log_event_bus.get()
                    for log in self._registered_loggers:
                        # TODO: schedule the update as a task instead of awaiting
                        await log.update(m)
                if root_event.is_set():
                    is_failed = self._states[ExecutionState.FAILED] or self._states[ExecutionState.DEFAULTED]
                    for log in self._registered_loggers:
                        if is_failed:
                            await log.on_failure()
                        else:
                            await log.on_success()
                        await log.on_destroy()
                    break
                await asyncio.sleep(self._observer_interval_seconds)

        return [asyncio.create_task(_pusher())]

    def _init_observers(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        if self._test:
            self._registered_observers = []

        async def _pusher() -> None:
            for obs in self._registered_observers:
                await obs.on_create()
            while True:
                while not self._execution_event_bus.empty():
                    e = self._execution_event_bus.get()
                    if self._debug:
                        print(e)
                    if isinstance(e, ExecutionStateTransition):
                        self._states[e.to_state].add(e.name)
                        self._states[e.from_state].remove(e.name)
                    for obs in self._registered_observers:
                        await obs.update(e)
                if root_event.is_set():
                    is_failed = self._states[ExecutionState.FAILED] or self._states[ExecutionState.DEFAULTED]
                    for obs in self._registered_observers:
                        if is_failed:
                            await obs.on_failure()
                        else:
                            await obs.on_success()
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
            log_event_bus=self._log_event_bus,
            execution_event_bus=self._execution_event_bus,
            shared_dict=self._shared_dict,
            await_dependencies=await_dependencies,
            semaphore=self._semaphore
        )

        self._executors[name] = ExecutorDetails(instance=e, dependencies=(deps or []))
        self._states[ExecutionState.INIT].add(name)

    def load_job_definition(self, j: Union[JobDefinition, str], filetype: str = 'yaml') -> Flowmancer:
        if isinstance(j, JobDefinition):
            jobdef = j
        else:
            jobdef = _job_definition_classes[filetype]().load(j)

        for n, t in jobdef.tasks.items():
            self.add_executor(
                name=n,
                task_class=t.task,
                deps=t.dependencies
            )

        for n, o in jobdef.observers.items():
            self._registered_observers.append(
                _observer_classes[o.observer](**o.kwargs)
            )

        return self

    def get_job_definition(self) -> JobDefinition:
        j = JobDefinition(tasks=dict())
        for n, e in self._executors.items():
            j.tasks[n] = (
                TaskDefinition(
                    task=type(e.instance.get_task_instance()).__name__,
                    dependencies=[]
                )
            )
        return j
