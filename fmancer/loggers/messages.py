from __future__ import annotations

from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel

_message_classes = dict()


def message(t: type[SerializableMessage]) -> Any:
    if not issubclass(t, SerializableMessage):
        raise TypeError(f'Must extend `SerializableEvent` type: {t.__name__}')
    _message_classes[t.__name__] = t
    return t


class Severity(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class SerializableMessage(BaseModel):
    class Config:
        use_enum_values = True

    def serialize(self) -> Dict[str, Any]:
        return {"message": type(self).__name__, "body": self.dict()}

    @classmethod
    def deserialize(cls, e: Dict[str, Any]) -> Any:
        if "message" not in e or "body" not in e or not isinstance(e, dict):
            return UnknownMessage(content=str(e))
        else:
            return _message_classes[e["message"]](**e["body"])


@message
class LogStartMessage(SerializableMessage):
    name: str


@message
class LogEndMessage(SerializableMessage):
    name: str


@message
class LogMessage(SerializableMessage):
    name: str
    severity: Severity
    message: str


@message
class UnknownMessage(SerializableMessage):
    content: str
