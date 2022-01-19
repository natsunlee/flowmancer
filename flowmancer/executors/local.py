import asyncio, multiprocessing
from .executor import Executor
from ..tasks.task import Task

class LocalExecutor(Executor):

    async def execute(self, task: Task) -> None:
        proc = multiprocessing.Process(target=task.run_lifecycle, daemon=False)
        proc.start()
        loop = asyncio.get_running_loop()
        await asyncio.gather(loop.run_in_executor(None, proc.join))