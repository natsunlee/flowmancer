from __future__ import annotations

from enum import Enum
from typing import Dict, Set, Union

from . import SerializableEvent, serializable_event


class SerializableExecutionEvent(SerializableEvent):
    @classmethod
    def event_group(cls) -> str:
        return 'SerializableExecutionEvent'


class ExecutionState(str, Enum):
    FAILED = 'F'
    PENDING = 'P'
    RUNNING = 'R'
    DEFAULTED = 'D'
    COMPLETED = 'C'
    ABORTED = 'A'
    SKIP = 'S'
    INIT = '_'


class ExecutionStateMap:
    def __init__(self) -> None:
        self.data: Dict[ExecutionState, Set[str]] = dict()

    def __getitem__(self, k: Union[str, ExecutionState]) -> Set[str]:
        es = ExecutionState(k)
        if es not in self.data:
            self.data[es] = set()
        return self.data[ExecutionState(k)]

    def __setitem__(self, k: Union[str, ExecutionState], v: Set[str]) -> None:
        es = ExecutionState(k)
        self.data[es] = v

    def __str__(self):
        return str(self.data)

    def items(self):
        return self.data.items()

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    @classmethod
    def from_simple_dict(cls, data: Dict[str, Set[str]]) -> ExecutionStateMap:
        m = ExecutionStateMap()
        for k, v in data.items():
            m[ExecutionState(k)] = v
        return m

    def to_simple_dict(self) -> Dict[str, Set[str]]:
        return {k.value: v for k, v in self.data.items()}


@serializable_event
class ExecutionStateTransition(SerializableExecutionEvent):
    name: str
    from_state: ExecutionState
    to_state: ExecutionState
