import asyncio
from abc import ABC, abstractmethod
from typing import Dict
from ..executor import Executor

class Watcher(ABC):
    def __init__(self, executors: Dict[str, Executor]) -> None:
        self._event = asyncio.Event()
        self.executors = executors.copy()

    @abstractmethod
    async def start(self) -> None:
        pass