import asyncio, multiprocessing, time, importlib
from typing import Callable
from .typedefs.enums import ExecutionState
from .tasks.task import Task
from .typedefs.models import LoggersDefinition, TaskDefinition
from .logmanager import LogManager

class Executor:
    def __init__(
        self, name: str,
        taskdef: TaskDefinition,
        logsdef: LoggersDefinition,
        resolve_dependency: Callable,
        restore_state: ExecutionState = None
    ) -> None:
        self._event: asyncio.Event = None
        self.name = name
        self._logger = LogManager(name, logsdef)
        self._resolve_dependency = resolve_dependency

        self.state = ExecutionState.PENDING
        if restore_state in (ExecutionState.COMPLETED, ExecutionState.SKIP):
            self.state = restore_state

        self.dependencies = taskdef.dependencies
        self.module = taskdef.module
        self.task = taskdef.task
        self.max_attempts = taskdef.max_attempts
        self.backoff = taskdef.backoff
        self._attempts = 0
    
    @property
    def is_alive(self) -> bool:
        if self._event is None:
            return True
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
        self._event = asyncio.Event()

        # In the event of a restart and this task is already complete, return immediately.
        if self.state == ExecutionState.COMPLETED:
            self._event.set()
            return

        # Wait for the completion of prior/dependency tasks.
        for dep_name in self.dependencies:
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

        task = self.TaskClass(self._logger)

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