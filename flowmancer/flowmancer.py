from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
import os
import pkgutil
import sys
import time
from argparse import ArgumentParser
from collections import namedtuple
from multiprocessing import Manager
from multiprocessing.managers import DictProxy
from typing import Any, Dict, List, Optional, Type, Union, cast

from pydantic import BaseModel

from .checkpoint import CheckpointContents, NoCheckpointAvailableError
from .checkpoint.file import FileCheckpoint
from .eventbus import EventBus
from .eventbus.execution import ExecutionState, ExecutionStateTransition, SerializableExecutionEvent
from .eventbus.log import SerializableLogEvent
from .executor import ExecutionStateMap, Executor
from .extensions.extension import Extension, _extension_classes
from .jobdefinition import (
    Configuration,
    ExtensionDefinition,
    JobDefinition,
    LoggerDefinition,
    TaskDefinition,
    _job_definition_classes,
)
from .loggers.logger import Logger, _logger_classes
from .task import Task

__all__ = ['Flowmancer']

ExecutorDetails = namedtuple('ExecutorDetails', 'instance dependencies')


class NoTasksLoadedError(Exception):
    pass


# Need to explicitly manage loop in case multiple instances of Flowmancer are run.
@contextlib.contextmanager
def _create_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.stop()
    loop.close()


def _load_extensions_path(path: str, add_to_path: bool = True, package_chain: List[str] = []):
    if not path.startswith('/'):
        path = os.path.join(os.path.dirname(os.path.abspath(inspect.stack()[-1][1])), path)
    if add_to_path:
        sys.path.append(path)
    for x in pkgutil.iter_modules(path=[path]):
        importlib.import_module('.'.join(package_chain+[x.name]))
        if x.ispkg:
            _load_extensions_path(os.path.join(path, x.name), False, package_chain+[x.name])


class Flowmancer:
    def __init__(self, *, test: bool = False, debug: bool = False) -> None:
        manager = Manager()
        self._config: Configuration = Configuration()
        self._test = test
        self._debug = debug
        self._log_event_bus = EventBus[SerializableLogEvent](manager.Queue())
        self._execution_event_bus = EventBus[SerializableExecutionEvent]()
        self._shared_dict: DictProxy[str, Any] = manager.dict()
        self._executors: Dict[str, ExecutorDetails] = dict()
        self._states = ExecutionStateMap()
        self._registered_extensions: Dict[str, Extension] = dict()
        self._registered_loggers: Dict[str, Logger] = dict()
        self._checkpoint_interval_seconds = 10
        self._tick_interval_seconds = 0.25

    def start(self) -> int:
        orig_cwd = os.getcwd()
        try:
            # Ensure any components, such as file loggers, work with respect to the caller's project dir.
            os.chdir(os.path.dirname(os.path.abspath(inspect.stack()[-1][1])))
            if not self._test:
                self._process_cmd_args()
            if not self._executors:
                raise NoTasksLoadedError(
                    'No Tasks have been loaded! Please check that you have provided a valid Job Definition file.'
                )
            ret = asyncio.run(self._initiate())
            return ret
        finally:
            os.chdir(orig_cwd)

    async def _initiate(self) -> int:
        with _create_loop():
            root_event = asyncio.Event()
            if self._config.max_concurrency > 0:
                semaphore = asyncio.Semaphore(self._config.max_concurrency)
                for i in self._executors.values():
                    i.instance.semaphore = semaphore
            observer_tasks = self._init_extensions(root_event)
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
        parser.add_argument('--max-concurrency', action='store', type=int, dest='max_concurrency')

        args = parser.parse_args()
        self._debug = args.debug

        if args.jobdef:
            self.load_job_definition(args.jobdef)

        if args.restart:
            try:
                cp = FileCheckpoint(checkpoint_name=self._config.name).read_checkpoint()
                self._shared_dict.update(cp.shared_dict)
                for name in cp.states[ExecutionState.FAILED]:
                    self._executors[name].instance.is_restart = True
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
                # Even though completed and don't require to be executed again, these tasks still need to trigger
                # state change from INIT -> COMPLETED for components watching the Execution Event Bus.
                for n in completed:
                    self._executors[n].instance.state = ExecutionState.COMPLETED
            except NoCheckpointAvailableError:
                print(f"No checkpoint file found for '{self._config.name}'. Starting new job.")

        # These override settings from JobsDefinition, if also defined there.
        if args.max_concurrency is not None:
            self._config.max_concurrency = args.max_concurrency

    # ASYNC INITIALIZATIONS
    def _init_checkpointer(self, root_event) -> asyncio.Task:
        async def _write_checkpoint() -> None:
            checkpoint = FileCheckpoint(checkpoint_name=self._config.name)
            last_write = 0
            while True:
                if (time.time() - last_write) >= self._checkpoint_interval_seconds:
                    checkpoint.write_checkpoint(
                        CheckpointContents(
                            name=self._config.name,
                            states=self._states,
                            shared_dict=self._shared_dict.copy()
                        )
                    )
                if root_event.is_set():
                    if (
                        not self._states[ExecutionState.FAILED]
                        and not self._states[ExecutionState.DEFAULTED]
                        and not self._states[ExecutionState.ABORTED]
                    ):
                        checkpoint.clear_checkpoint()
                    break
                else:
                    await asyncio.sleep(self._tick_interval_seconds)

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
                    await asyncio.sleep(self._tick_interval_seconds)

        return [asyncio.create_task(_synchro())] + [
            asyncio.create_task(dtl.instance.start())
            for name, dtl in self._executors.items()
            if name in self._states[ExecutionState.INIT]
        ]

    def _init_loggers(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        if self._test:
            self._registered_loggers = dict()

        async def _pusher() -> None:
            for log in self._registered_loggers.values():
                await log.on_create()
            while True:
                while not self._log_event_bus.empty():
                    m = self._log_event_bus.get()
                    for log in self._registered_loggers.values():
                        # TODO: schedule the update as a task instead of awaiting
                        await log.update(m)
                if root_event.is_set():
                    is_failed = self._states[ExecutionState.FAILED] or self._states[ExecutionState.DEFAULTED]
                    for log in self._registered_loggers.values():
                        if is_failed:
                            await log.on_failure()
                        else:
                            await log.on_success()
                        await log.on_destroy()
                    break
                await asyncio.sleep(self._tick_interval_seconds)

        return [asyncio.create_task(_pusher())]

    def _init_extensions(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        if self._test:
            self._registered_extensions = dict()

        async def _pusher() -> None:
            for obs in self._registered_extensions.values():
                await obs.on_create()
            while True:
                while not self._execution_event_bus.empty():
                    e = self._execution_event_bus.get()
                    if self._debug:
                        print(e)
                    if isinstance(e, ExecutionStateTransition):
                        self._states[e.to_state].add(e.name)
                        self._states[e.from_state].remove(e.name)
                    for obs in self._registered_extensions.values():
                        await obs.update(e)
                if root_event.is_set():
                    is_failed = self._states[ExecutionState.FAILED] or self._states[ExecutionState.DEFAULTED]
                    for obs in self._registered_extensions.values():
                        if is_failed:
                            await obs.on_failure()
                        else:
                            await obs.on_success()
                        await obs.on_destroy()
                    break
                await asyncio.sleep(self._tick_interval_seconds)

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

    def add_executor(
        self,
        name: str,
        task_class: Union[str, Type[Task]],
        deps: Optional[List[str]] = None,
        max_attempts: int = 1,
        backoff: int = 0,
        parameters: Dict[str, Any] = dict()
    ) -> None:
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
            max_attempts=max_attempts,
            backoff=backoff,
            parameters=parameters
        )

        self._executors[name] = ExecutorDetails(instance=e, dependencies=(deps or []))
        self._states[ExecutionState.INIT].add(name)

    def load_job_definition(self, j: Union[JobDefinition, str], filetype: str = 'yaml') -> Flowmancer:
        if isinstance(j, JobDefinition):
            jobdef = j
        else:
            jobdef = _job_definition_classes[filetype]().load(j)

        # Configurations
        self._config = jobdef.config

        # Recursively import any modules found in the following paths in order to trigger the registration of any
        # decorated classes.
        search_paths = ['./tasks', './extensions', './loggers'] + jobdef.config.extension_directories
        for p in search_paths:
            _load_extensions_path(p)

        for p in jobdef.config.extension_packages:
            importlib.import_module(p)

        # Tasks
        for n, t in jobdef.tasks.items():
            self.add_executor(
                name=n,
                task_class=t.task,
                deps=t.dependencies,
                max_attempts=t.max_attempts,
                backoff=t.backoff,
                parameters=t.parameters
            )

        # Observers
        for n, e in jobdef.extensions.items():
            self._registered_extensions[n] = _extension_classes[e.extension](**e.parameters)

        # Loggers
        for n, l in jobdef.loggers.items():
            self._registered_loggers[n] = _logger_classes[l.logger](**l.parameters)

        return self

    def get_job_definition(self) -> JobDefinition:
        j = JobDefinition(tasks=dict())
        j.config = self._config

        for n, e in self._executors.items():
            j.tasks[n] = TaskDefinition(
                task=type(e.instance.get_task_instance()).__name__,
                dependencies=[],
                parameters=e.instance.get_task_instance().dict()
            )

        for n, e in self._registered_extensions.items():
            j.extensions[n] = ExtensionDefinition(
                extension=type(e).__name__,
                parameters=cast(BaseModel, e).dict()
            )

        for n, l in self._registered_loggers.items():
            j.loggers[n] = LoggerDefinition(
                logger=type(l).__name__,
                parameters=cast(BaseModel, l).dict()
            )

        return j
