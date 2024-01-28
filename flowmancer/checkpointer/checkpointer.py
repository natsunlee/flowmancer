from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

from pydantic import BaseModel, Extra

from ..executor import ExecutionStateMap

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
    states: ExecutionStateMap
    shared_dict: Dict[Any, Any]


class Checkpointer(ABC, BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True

    @abstractmethod
    def write_checkpoint(self, name: str, content: CheckpointContents) -> None:
        pass

    @abstractmethod
    def read_checkpoint(self, name: str) -> CheckpointContents:
        pass

    @abstractmethod
    def clear_checkpoint(self, name: str) -> None:
        pass
