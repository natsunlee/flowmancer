import asyncio, multiprocessing
from .executor import Executor
from ..tasks.task import Task

class LocalExecutor(Executor):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.proc = None

    async def execute(self, task: Task) -> None:
        self.proc = multiprocessing.Process(target=task.run_lifecycle, daemon=False)
        self.proc.start()
        loop = asyncio.get_running_loop()
        await asyncio.gather(loop.run_in_executor(None, self.proc.join))
    
    def terminate(self) -> None:
        if self.proc is not None:
            # Send SIGTERM to child process
            self.proc.terminate()