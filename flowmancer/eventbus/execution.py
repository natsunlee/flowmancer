from enum import Enum

from . import SerializableEvent, serializable_event


class SerializableExecutionEvent(SerializableEvent):
    @classmethod
    def event_group(cls) -> str:
        return 'SerializableExecutionEvent'


class ExecutionState(Enum):
    FAILED = 'F'
    PENDING = 'P'
    RUNNING = 'R'
    DEFAULTED = 'D'
    COMPLETED = 'C'
    ABORTED = 'A'
    SKIP = 'S'
    INIT = '_'


@serializable_event
class ExecutionStateTransition(SerializableExecutionEvent):
    name: str
    from_state: ExecutionState
    to_state: ExecutionState
