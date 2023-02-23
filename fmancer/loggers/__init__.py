from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict

from pydantic import BaseModel

from ..lifecycle import AsyncLifecycle

_event_classes = dict()
_logger_classes = dict()


def log_event(t: type[SerializableLogEvent]) -> Any:
    if not issubclass(t, SerializableLogEvent):
        raise TypeError(f'Must extend `SerializableLogEvent` type: {t.__name__}')
    _event_classes[t.__name__] = t
    return t


def logger(t: type[Logger]) -> Any:
    if not issubclass(t, Logger):
        raise TypeError(f'Must extend `Logger` type: {t.__name__}')
    _logger_classes[t.__name__] = t
    return t


class Severity(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class SerializableLogEvent(BaseModel):
    class Config:
        use_enum_values = True

    def serialize(self) -> Dict[str, Any]:
        return {"message": type(self).__name__, "body": self.dict()}

    @classmethod
    def deserialize(cls, e: Dict[str, Any]) -> Any:
        if "message" not in e or "body" not in e or not isinstance(e, dict):
            return UnknownLogEvent(content=str(e))
        else:
            return _event_classes[e["message"]](**e["body"])


@log_event
class LogStartEvent(SerializableLogEvent):
    name: str


@log_event
class LogEndEvent(SerializableLogEvent):
    name: str


@log_event
class LogWriteEvent(SerializableLogEvent):
    name: str
    severity: Severity
    message: str


@log_event
class UnknownLogEvent(SerializableLogEvent):
    content: str


class Logger(ABC, AsyncLifecycle):
    @abstractmethod
    async def update(self, m: SerializableLogEvent) -> None:
        pass
