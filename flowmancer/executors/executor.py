import asyncio, time, importlib
from abc import ABC, abstractmethod
from typing import Callable, List
from ..typedefs.enums import ExecutionState
from ..tasks.task import Task
from ..typedefs.models import LoggersDefinition, TaskDefinition
from ..managers.logmanager import LogManager

class Executor(ABC):

    semaphore: asyncio.Semaphore = None
    restart = False

    def __init__(
        self,
        name: str,
        taskdef: TaskDefinition,
        logsdef: LoggersDefinition,
        resolve_dependency: Callable,
        notify_state_transition: Callable,
        stash: dict = None
    ) -> None:
        self._event: asyncio.Event = None
        self.name = name
        self._logger = LogManager(name, logsdef)
        self._resolve_dependency = resolve_dependency
        self._notify_state_transition = notify_state_transition
        self._state = ExecutionState.PENDING
        self._taskdef = taskdef
        self.stash = stash or dict()
        self._attempts = 0

    @property
    def state(self) -> ExecutionState:
        return self._state
    @state.setter
    def state(self, val: ExecutionState) -> None:
        self._notify_state_transition(self.name, self._state, val)
        self._state = val
    
    @property
    def is_restart(self) -> bool:
        return self.__class__.restart

    @property
    def dependencies(self) -> List[str]:
        return self._taskdef.dependencies

    @property
    def is_alive(self) -> bool:
        if self._event is None:
            return True
        return not self._event.is_set()

    @property
    def TaskClass(self) -> Task:
        task_class = getattr(importlib.import_module(self._taskdef.module), self._taskdef.task)
        if not issubclass(task_class, Task):
            raise TypeError(f"{self._taskdef.module}.{self._taskdef.task} is not an extension of Task")
        return task_class
    
    async def wait(self) -> None:
        await self._event.wait()

    async def start(self) -> None:
        self._event = asyncio.Event()
        task = self.TaskClass(
            self.stash,
            self._logger,
            self._taskdef.args,
            self._taskdef.kwargs
        )

        # In the event of a restart and this task is already complete, return immediately.
        if self.state == ExecutionState.COMPLETED:
            self._event.set()
            return

        # Wait for the completion of prior/dependency tasks.
        for dep_name in self._taskdef.dependencies:
            d = self._resolve_dependency(dep_name)
            await d.wait()
            if d.state == ExecutionState.FAILED:
                self.state = ExecutionState.DEFAULTED
                self._event.set()
                return
        
        # In the event of skipped task, return immediately.
        if self.state == ExecutionState.SKIP:
            self._event.set()
            return
        
        while self._attempts < self._taskdef.max_attempts and self.state == ExecutionState.PENDING:
            if self.__class__.semaphore: await self.__class__.semaphore.acquire()
            self.state = ExecutionState.RUNNING
            self._start_time = time.time()
            self._attempts += 1

            await self.execute(task)
            self._end_time = time.time()
            if self.__class__.semaphore: self.__class__.semaphore.release()

            if task.is_failed and (self._attempts < self._taskdef.max_attempts):
                self.state = ExecutionState.PENDING
                await asyncio.sleep(self._taskdef.backoff)

        self.state = ExecutionState.FAILED if task.is_failed else ExecutionState.COMPLETED
        self._event.set()
    
    @abstractmethod
    async def execute(self, task: Task) -> None:
        pass