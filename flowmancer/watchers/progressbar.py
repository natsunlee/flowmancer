import asyncio
from .watcher import Watcher
from tqdm import tqdm

class ProgressBar(Watcher):
    async def start(self) -> None:
        pending = set(self.executors.values())
        total = len(pending)
        with tqdm(total=total) as pbar:
            while pending:
                for ex in pending.copy():
                    if not ex.is_alive:
                        pbar.update(1)
                        pending.remove(ex)
                    else:
                        pbar.update(0)
                await asyncio.sleep(1)
        self._event.set()