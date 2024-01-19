from __future__ import annotations

from enum import Enum
from typing import Iterable, Optional

from . import EventBus, SerializableEvent, serializable_event


class Severity(Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class SerializableLogEvent(SerializableEvent):
    @classmethod
    def event_group(cls) -> str:
        return 'SerializableLogEvent'


@serializable_event
class LogStartEvent(SerializableLogEvent):
    name: str


@serializable_event
class LogEndEvent(SerializableLogEvent):
    name: str


@serializable_event
class LogWriteEvent(SerializableLogEvent):
    name: str
    severity: Severity
    message: str


@serializable_event
class UnknownLogEvent(SerializableLogEvent):
    content: str


class LogWriter:
    __slots__ = ('b', 'n')

    def __init__(self, n: str, b: Optional[EventBus[SerializableLogEvent]]) -> None:
        self.n = n
        self.b = b
        if self.b:
            self.b.put(LogStartEvent(name=self.n))

    def write(self, m: str) -> None:
        if self.b:
            self.b.put(LogWriteEvent(name=self.n, severity=Severity.INFO, message=m))

    def writelines(self, mlist: Iterable[str]) -> None:
        for m in mlist:
            self.write(m)

    def flush(self) -> None:
        # Need to imitate file-like object for redirecting stdout
        pass

    def close(self) -> None:
        if self.b:
            self.b.put(LogEndEvent(name=self.n))
