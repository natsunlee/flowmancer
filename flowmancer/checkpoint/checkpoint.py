from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict

from pydantic import BaseModel, Extra

from ..executor import ExecutionStateMap

_checkpoint_classes = dict()


def checkpoint(t: type[Checkpoint]) -> Any:
    if not issubclass(t, Checkpoint):
        raise TypeError(f'Must extend `Checkpoint` type: {t.__name__}')
    _checkpoint_classes[t.__name__] = t
    return t


class NoCheckpointAvailableError(Exception):
    pass


@dataclass
class CheckpointContents:
    name: str
    states: ExecutionStateMap
    shared_dict: Dict[Any, Any]


class Checkpoint(ABC, BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True

    @abstractmethod
    def write_checkpoint(self, content: CheckpointContents) -> None:
        pass

    @abstractmethod
    def read_checkpoint(self) -> CheckpointContents:
        pass

    @abstractmethod
    def clear_checkpoint(self) -> None:
        pass
