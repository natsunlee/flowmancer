from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel

from .enums import ExecutionState

_event_classes = dict()


def event(t: type[SerializableEvent]) -> Any:
    if not issubclass(t, SerializableEvent):
        raise TypeError(f'Must extend `SerializableEvent` type: {t.__name__}')
    _event_classes[t.__name__] = t
    return t


class SerializableEvent(BaseModel):
    class Config:
        use_enum_values = True

    def serialize(self) -> Dict[str, Any]:
        return {"event": type(self).__name__, "body": self.dict()}

    @classmethod
    def deserialize(cls, e: Dict[str, Any]) -> Any:
        if "event" not in e or "body" not in e or not isinstance(e, dict):
            return UnknownEvent(content=str(e))
        else:
            return _event_classes[e["event"]](**e["body"])


@event
class ExecutionStateTransition(SerializableEvent):
    name: str
    from_state: ExecutionState
    to_state: ExecutionState


@event
class UnknownEvent(SerializableEvent):
    content: str
