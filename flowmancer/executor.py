import asyncio, multiprocessing, time, importlib
from typing import List
from .typedefs.enums import ExecutionState
from .typedefs.exceptions import DuplicateDependency
from .tasks.task import Task
from .logger.logger import Logger

class Executor:
    def __init__(self, name: str) -> None:
        self._event = asyncio.Event()
        self._dependencies: List["Executor"] = []
        self._state = ExecutionState.PENDING
        self.logger: Logger
        self.name = name

        self.max_attempts = 1
        self.backoff = 0
        self._attempts = 0

        self.module: str
        self.task: str
    
    def __hash__(self) -> int:
        return hash(self.name)
    def __eq__(self, other) -> bool:
        return self.name == other.name

    def add_dependency(self, dep: "Executor") -> None:
        if type(dep) != type(self):
            raise TypeError(f"Dependency must be of 'Executor' type.")
        if dep in self._dependencies:
            raise DuplicateDependency(f"Dependency '{dep.name}' already exists for '{self.name}'.")
        self._dependencies.append(dep)

    @property
    def state(self) -> ExecutionState:
        return self._state
    @state.setter
    def state(self, val: ExecutionState) -> None:
        if not isinstance(val, ExecutionState):
            raise TypeError("Value for 'state' must be of ExecutionState type.")
        self._state = val
    
    @property
    def is_alive(self) -> bool:
        return not self._event.is_set()

    @property
    def TaskClass(self) -> Task:
        task_class = getattr(importlib.import_module(self.module), self.task)
        if not issubclass(task_class, Task):
            raise TypeError(f"{self.module}.{self.task} is not an extension of Task")
        return task_class
    
    async def wait(self) -> None:
        await self._event.wait()

    async def start(self) -> None:
        for d in self._dependencies:
            await d.wait()
            if d.state == ExecutionState.FAILED:
                self.state = ExecutionState.DEFAULTED
                self._event.set()
                return

        task = self.TaskClass(self.logger)

        while self._attempts < self.max_attempts and self.state == ExecutionState.PENDING:
            self.state = ExecutionState.RUNNING
            self._start_time = time.time()
            self._attempts += 1

            proc = multiprocessing.Process(target=task.run_lifecycle, daemon=False)
            proc.start()
            loop = asyncio.get_running_loop()
            await asyncio.gather(loop.run_in_executor(None, proc.join))
            self._end_time = time.time()
            
            if task.is_failed and (self._attempts < self.max_attempts):
                await asyncio.sleep(self.backoff)

        self.state = ExecutionState.FAILED if task.is_failed else ExecutionState.COMPLETED
        self._event.set()