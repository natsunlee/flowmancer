import asyncio
from multiprocessing import Process
from typing import Optional

from ..tasks.task import Task
from .executor import Executor


class LocalExecutor(Executor):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.proc: Optional[Process] = None

    async def execute(self, task: Task) -> None:
        self.proc = Process(target=task.run_lifecycle, daemon=False)
        self.proc.start()
        loop = asyncio.get_running_loop()
        await asyncio.gather(loop.run_in_executor(None, self.proc.join))

    def terminate(self) -> None:
        if self.proc is not None:
            # Send SIGTERM to child process
            self.proc.terminate()
