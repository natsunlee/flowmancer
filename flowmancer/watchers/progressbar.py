import asyncio
from .watcher import Watcher
from tqdm import tqdm
from ..typedefs.enums import ExecutionState

class ProgressBar(Watcher):
    async def start(self) -> None:
        pending = set(self.executors.values())
        total = len(pending)
        failed = 0
    
        with tqdm(total=total) as pbar:
            while pending:
                running = 0
                for ex in pending.copy():
                    if not ex.is_alive:
                        pbar.update(1)
                        pending.remove(ex)
                        if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                            failed += 1
                    else:
                        pbar.update(0)
                        if ex.state == ExecutionState.RUNNING: running += 1
                pbar.set_description(f"Running: {running} - Failed: {failed}")
                await asyncio.sleep(0.5)
        self._event.set()