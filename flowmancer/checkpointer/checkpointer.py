from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Set

from pydantic import BaseModel, ConfigDict

from ..lifecycle import AsyncLifecycle

_checkpointer_classes = dict()


def checkpointer(t: type[Checkpointer]) -> Any:
    if not issubclass(t, Checkpointer):
        raise TypeError(f'Must extend `Checkpointer` type: {t.__name__}')
    _checkpointer_classes[t.__name__] = t
    return t


class NoCheckpointAvailableError(Exception):
    pass


@dataclass
class CheckpointContents:
    name: str
    states: Dict[str, Set[str]]
    shared_dict: Dict[Any, Any]


class Checkpointer(ABC, BaseModel, AsyncLifecycle):
    model_config = ConfigDict(extra='forbid')

    @abstractmethod
    async def write_checkpoint(self, name: str, content: CheckpointContents) -> None:
        pass

    @abstractmethod
    async def read_checkpoint(self, name: str) -> CheckpointContents:
        pass

    @abstractmethod
    async def clear_checkpoint(self, name: str) -> None:
        pass
