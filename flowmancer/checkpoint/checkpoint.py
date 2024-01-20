from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Type

from ..executor import ExecutionStateMap
from ..jobdefinition import JobDefinition

_checkpoint_classes = dict()


def checkpoint(t: type[Checkpoint]) -> Type[Checkpoint]:
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
    job_definition: JobDefinition
    shared_dict: Dict[Any, Any]


class Checkpoint(ABC):
    def __init__(self, **_) -> None:
        pass

    @abstractmethod
    def write_checkpoint(self, content: CheckpointContents) -> None:
        pass

    @abstractmethod
    def read_checkpoint(self) -> CheckpointContents:
        pass

    @abstractmethod
    def clear_checkpoint(self) -> None:
        pass
