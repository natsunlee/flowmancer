import asyncio
from .watcher import Watcher

class Snapshot(Watcher):
    async def start(self):
        while not self.stop:
            await asyncio.sleep(1)