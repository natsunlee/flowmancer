from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
import json
import os
import pkgutil
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from multiprocessing import Manager
from multiprocessing.managers import DictProxy
from typing import Any, Dict, List, Optional, Type, Union, cast

from pydantic import BaseModel, ValidationError

from .checkpointer import CheckpointContents, Checkpointer, NoCheckpointAvailableError
from .checkpointer.checkpointer import _checkpointer_classes
from .checkpointer.file import FileCheckpointer
from .eventbus import EventBus
from .eventbus.execution import ExecutionState, ExecutionStateMap, ExecutionStateTransition, SerializableExecutionEvent
from .eventbus.log import SerializableLogEvent
from .exceptions import (
    CheckpointInvalidError,
    ExtensionsDirectoryNotFoundError,
    ModuleLoadError,
    NotAPackageError,
    NoTasksLoadedError,
    TaskValidationError,
    VarFormatError,
)
from .executor import Executor
from .extensions.extension import Extension, _extension_classes
from .jobdefinition import (
    ConfigurationDefinition,
    ExtensionDefinition,
    JobDefinition,
    LoadParams,
    LoggerDefinition,
    TaskDefinition,
    _job_definition_classes,
)
from .loggers.logger import Logger, _logger_classes
from .task import Task

__all__ = ['Flowmancer']


@dataclass
class ExecutorDetails:
    instance: Executor
    dependencies: List[str]


# Need to explicitly manage loop in case multiple instances of Flowmancer are run.
@contextlib.contextmanager
def _create_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.stop()
    loop.close()


def _load_extensions_path(path: str, package_chain: Optional[List[str]] = None) -> None:
    if not path.startswith('/'):
        path = os.path.abspath(
            os.path.join(
                os.path.dirname(
                    os.path.abspath(inspect.stack()[-1][1])
                ),
                path
            )
        )

    if not os.path.exists(path):
        raise ExtensionsDirectoryNotFoundError(f"No such directory: '{path}'")
    if os.path.isfile(path):
        raise NotAPackageError(f"Only packages (directories) are allowed. The following is not a dir: '{path}'")
    if not os.path.exists(os.path.join(path, '__init__.py')):
        print(f"WARNING: The '{path}' dir is not a package (no __init__.py file found). Modules will not be imported.")
        return None

    if not package_chain:
        package_chain = [os.path.basename(path)]

    for x in pkgutil.iter_modules(path=[path]):
        try:
            print(f"Loading Module: {'.'.join(package_chain+[x.name])}")
            importlib.import_module('.'.join(package_chain+[x.name]))
        except Exception as e:
            raise ModuleLoadError(f"Error loading '{'.'.join(package_chain+[x.name])}': {e}")
        if x.ispkg:
            _load_extensions_path(os.path.join(path, x.name), package_chain+[x.name])


class Flowmancer:
    def __init__(self, *, test: bool = False, debug: bool = False) -> None:
        manager = Manager()
        self._config: ConfigurationDefinition = ConfigurationDefinition()
        self._test = test
        self._debug = debug
        self._log_event_bus = EventBus[SerializableLogEvent](self._config.name, manager.Queue())
        self._execution_event_bus = EventBus[SerializableExecutionEvent](self._config.name)
        self._shared_dict: DictProxy[str, Any] = manager.dict()
        self._executors: Dict[str, ExecutorDetails] = dict()
        self._states = ExecutionStateMap()
        self._registered_extensions: Dict[str, Extension] = dict()
        self._registered_loggers: Dict[str, Logger] = dict()
        self._checkpointer_instance: Checkpointer = FileCheckpointer()
        self._checkpointer_interval_seconds = 10.0
        self._extensions_interval_seconds = 0.25
        self._loggers_interval_seconds = 0.25
        self._synchro_interval_seconds = 0.25
        self._is_restart = False
        self._jobdef_vars: Dict[str, str] = dict()

    def set_jobdef_var(self, key: str, value: str) -> None:
        if not isinstance(key, str):
            raise TypeError(f'str expected for `key`, not {type(key)}')
        if not isinstance(value, str):
            raise TypeError(f'str expected for `value`, not {type(value)}')
        self._jobdef_vars[key] = value

    def unset_jobdef_var(self, key: str) -> None:
        if not isinstance(key, str):
            raise TypeError(f'str expected for `key`, not {type(key)}')
        del self._jobdef_vars[key]

    def start(
        self,
        default_jobdef_path: Optional[str] = None,
        default_jobdef_type: str = 'yaml',
        raise_exception_on_failure: bool = False
    ) -> int:
        orig_cwd = os.getcwd()

        try:
            # Ensure any components, such as file loggers, work with respect to the .py file in which the `start`
            # command is invoked, which is usually the project root dir.
            app_root_dir = os.path.dirname(os.path.abspath(inspect.stack()[-1][1]))
            os.chdir(app_root_dir)
            if not self._test:
                self._process_cmd_args(orig_cwd, app_root_dir, default_jobdef_path, default_jobdef_type)
            if not self._executors:
                raise NoTasksLoadedError(
                    'No Tasks have been loaded! Please check that you have provided a valid Job Definition file.'
                )
            self._validate_tasks()
            ret = asyncio.run(self._initiate())
            return ret
        except ValidationError as e:
            if raise_exception_on_failure:
                raise
            print('ERROR: Errors exist in the provided JobDefinition:')
            error_list = json.loads(e.json())
            for err in error_list:
                print(f' - {".".join(err["loc"])}: {err["msg"]}')
            return 1
        except Exception as e:
            if raise_exception_on_failure:
                raise
            print(f'ERROR: {e}')
            return 99
        finally:
            os.chdir(orig_cwd)

    def _validate_tasks(self) -> None:
        err = TaskValidationError('Errors exist in the provided JobDefinition for one or more tasks.')
        for n, ex in self._executors.items():
            try:
                ex.instance.get_task_class()(**(ex.instance.parameters or {}))
            except ValidationError as e:
                for ve in json.loads(e.json()):
                    err.add_error(f'tasks.{n}.parameters.{".".join(ve["loc"])}', ve['msg'])
            except Exception as e:
                err.add_error(f'tasks.{n}', repr(e))
        if err.errors:
            raise err

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

    def _validate_checkpoint(self, checkpoint: CheckpointContents) -> None:
        cp = set().union(*list(checkpoint.states.values()))
        ex = set(self._executors.keys())
        missing_in_checkpoint = ex.difference(cp)
        missing_in_executors = cp.difference(ex)
        if cp != ex:
            msg = 'Task names in Checkpoint do not match registered task names!'
            if missing_in_checkpoint:
                msg += '\nTasks MISSING in Checkpoint:'
                for n in missing_in_checkpoint:
                    msg += f'\n  * {n}'
            if missing_in_executors:
                msg += '\nUNKNOWN tasks in Checkpoint:'
                for n in missing_in_executors:
                    msg += 'f\n  * {n}'
            raise CheckpointInvalidError('Task names in Checkpoint do not match registered task names:')

    def _process_cmd_args(
        self,
        caller_cwd: str,
        app_root_dir: str,
        default_jobdef_path: Optional[str] = None,
        default_jobdef_type: str = 'yaml'
    ) -> None:
        # If a relative path is given in `start()`, it should be relative to the `start()` call. This is in contrast
        # to path given from command line, in which case paths should be relative to where the command is executed.
        if default_jobdef_path and not default_jobdef_path.startswith('/'):
            default_jobdef_path = os.path.join(app_root_dir, default_jobdef_path)
        parser = ArgumentParser(description='Flowmancer job execution options.')
        parser.add_argument('-j', '--jobdef', action='store', dest='jobdef', default=default_jobdef_path)
        parser.add_argument('-t', '--type', action='store', dest='jobdef_type', default=default_jobdef_type)
        parser.add_argument('-r', '--restart', action='store_true', dest='restart', default=False)
        parser.add_argument('-d', '--debug', action='store_true', dest='debug', default=False)
        parser.add_argument('--skip', action='append', dest='skip', default=[])
        parser.add_argument('--run-to', action='store', dest='run_to')
        parser.add_argument('--run-from', action='store', dest='run_from')
        parser.add_argument('--max-concurrency', action='store', type=int, dest='max_concurrency')
        parser.add_argument('--var', action='append', dest='jobdef_vars', default=[])

        args = parser.parse_args()
        self._debug = args.debug

        for v in args.jobdef_vars:
            parts = v.split('=')
            if len(parts) <= 1:
                raise VarFormatError('`var` arguments must follow the pattern: <key>=<value>')
            self.set_jobdef_var(parts[0], '='.join(parts[1:]))

        if args.jobdef:
            jobdef_path = args.jobdef if args.jobdef.startswith('/') else os.path.join(caller_cwd, args.jobdef)
            self.load_job_definition(jobdef_path, app_root_dir, args.jobdef_type)

        if args.restart:
            try:
                cp = asyncio.run(self._checkpointer_instance.read_checkpoint(self._config.name))
                self._validate_checkpoint(cp)
                self._shared_dict.update(cp.shared_dict)
                esm = ExecutionStateMap.from_simple_dict(cp.states)
                for name in esm[ExecutionState.FAILED]:
                    self._executors[name].instance.is_restart = True
                completed = esm[ExecutionState.COMPLETED].copy()
                esm[ExecutionState.INIT].update(esm[ExecutionState.FAILED])
                esm[ExecutionState.INIT].update(esm[ExecutionState.ABORTED])
                esm[ExecutionState.INIT].update(esm[ExecutionState.RUNNING])
                esm[ExecutionState.INIT].update(esm[ExecutionState.PENDING])
                esm[ExecutionState.INIT].update(esm[ExecutionState.DEFAULTED])
                esm[ExecutionState.INIT].update(esm[ExecutionState.COMPLETED])
                esm[ExecutionState.FAILED].clear()
                esm[ExecutionState.ABORTED].clear()
                esm[ExecutionState.RUNNING].clear()
                esm[ExecutionState.PENDING].clear()
                esm[ExecutionState.DEFAULTED].clear()
                esm[ExecutionState.COMPLETED].clear()
                self._states = esm
                self._is_restart = True
                # Even though completed and don't require to be executed again, these tasks still need to trigger
                # state change from INIT -> COMPLETED for components watching the Execution Event Bus.
                for n in completed:
                    self._executors[n].instance.state = ExecutionState.COMPLETED
            except NoCheckpointAvailableError:
                self._is_restart = False
                print(f"No checkpoint file found for '{self._config.name}'. Starting new job.")

        # These override settings from JobsDefinition, if also defined there.
        if args.max_concurrency is not None:
            self._config.max_concurrency = args.max_concurrency

    def _is_failed(self) -> bool:
        return bool(
            self._states[ExecutionState.FAILED]
            or self._states[ExecutionState.DEFAULTED]
            or self._states[ExecutionState.ABORTED]
        )

    # ASYNC INITIALIZATIONS
    def _init_checkpointer(self, root_event) -> asyncio.Task:
        async def _write_checkpoint() -> None:
            await self._checkpointer_instance.write_checkpoint(
                self._config.name,
                CheckpointContents(
                    name=self._config.name,
                    states=self._states.to_simple_dict(),
                    shared_dict=self._shared_dict.copy()
                )
            )

        async def _pusher() -> None:
            last_write = 0
            await self._checkpointer_instance.on_create()
            if self._is_restart:
                await self._checkpointer_instance.on_restart()
            while True:
                if root_event.is_set():
                    break
                if (time.time() - last_write) >= self._checkpointer_interval_seconds:
                    await _write_checkpoint()
                await asyncio.sleep(self._synchro_interval_seconds)

            if self._is_failed():
                await _write_checkpoint()
                await self._checkpointer_instance.on_failure()
            else:
                await self._checkpointer_instance.on_success()
                await self._checkpointer_instance.clear_checkpoint(self._config.name)
            await self._checkpointer_instance.on_destroy()

        return asyncio.create_task(_pusher())

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
                    await asyncio.sleep(self._synchro_interval_seconds)

        for ex in self._executors.values():
            ex.instance.init_event()

        return [asyncio.create_task(_synchro())] + [
            asyncio.create_task(dtl.instance.start())
            for name, dtl in self._executors.items()
            if name in self._states[ExecutionState.INIT]
        ]

    def _init_loggers(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        if self._test:
            self._registered_loggers = dict()

        async def _write_logs() -> None:
            while not self._log_event_bus.empty():
                m = self._log_event_bus.get()
                for log in self._registered_loggers.values():
                    await log.update(m)

        async def _pusher() -> None:
            for log in self._registered_loggers.values():
                await log.on_create()
                if self._is_restart:
                    await log.on_restart()

            last_trigger = 0
            while True:
                if root_event.is_set():
                    break
                if (time.time() - last_trigger) >= self._loggers_interval_seconds:
                    await _write_logs()
                await asyncio.sleep(self._synchro_interval_seconds)

            await _write_logs()
            for log in self._registered_loggers.values():
                if self._is_failed():
                    await log.on_failure()
                else:
                    await log.on_success()
                await log.on_destroy()

        return [asyncio.create_task(_pusher())]

    def _init_extensions(self, root_event: asyncio.Event) -> List[asyncio.Task]:
        if self._test:
            self._registered_extensions = dict()

        async def _emit() -> None:
            while not self._execution_event_bus.empty():
                e = self._execution_event_bus.get()
                if self._debug:
                    print(e)
                if isinstance(e, ExecutionStateTransition):
                    self._states[e.to_state].add(e.name)
                    self._states[e.from_state].remove(e.name)
                for obs in self._registered_extensions.values():
                    await obs.update(e)

        async def _pusher() -> None:
            for obs in self._registered_extensions.values():
                await obs.on_create()
                if self._is_restart:
                    await obs.on_restart()

            last_trigger = 0
            while True:
                if root_event.is_set():
                    break
                if (time.time() - last_trigger) >= self._extensions_interval_seconds:
                    await _emit()
                await asyncio.sleep(self._synchro_interval_seconds)

            await _emit()
            for obs in self._registered_extensions.values():
                if self._is_failed():
                    await obs.on_failure()
                else:
                    await obs.on_success()
                await obs.on_destroy()

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
            parameters=parameters,
            depends_on=deps
        )

        self._executors[name] = ExecutorDetails(instance=e, dependencies=(deps or []))
        self._states[ExecutionState.INIT].add(name)

    def load_job_definition(
        self,
        j: Union[JobDefinition, str],
        app_root_dir: str,
        jobdef_type: str = 'yaml'
    ) -> Flowmancer:
        if isinstance(j, JobDefinition):
            jobdef = j
        else:
            jobdef = _job_definition_classes[jobdef_type]().load(
                j, LoadParams(APP_ROOT_DIR=app_root_dir), self._jobdef_vars
            )

        # Configurations
        self._config = jobdef.config
        self._synchro_interval_seconds = jobdef.config.synchro_interval_seconds
        self._loggers_interval_seconds = jobdef.config.loggers_interval_seconds
        self._extensions_interval_seconds = jobdef.config.extensions_interval_seconds
        self._checkpointer_interval_seconds = jobdef.config.checkpointer_interval_seconds

        self._log_event_bus.job_name = self._config.name
        self._execution_event_bus.job_name = self._config.name

        # Recursively import any modules found in the following paths in order to trigger the registration of any
        # decorated classes.
        for p in ['./tasks', './extensions', './loggers']:
            try:
                _load_extensions_path(p)
            except ExtensionsDirectoryNotFoundError:
                # Don't error on the absence of dirs that are searched by default.
                pass

        # Allow for missing dir exceptions for passed-in paths.
        for p in jobdef.config.extension_directories:
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

        # Checkpointer
        self._checkpointer_instance = _checkpointer_classes[jobdef.checkpointer.checkpointer](
            **jobdef.checkpointer.parameters
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
                parameters=e.instance.get_task_instance().model_dump()
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
