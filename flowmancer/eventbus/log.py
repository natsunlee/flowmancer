from __future__ import annotations

from enum import Enum
from typing import Iterable, Optional

from . import EventBus, SerializableEvent, serializable_event


class Severity(str, Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class SerializableLogEvent(SerializableEvent):
    name: str

    @classmethod
    def event_group(cls) -> str:
        return 'SerializableLogEvent'


@serializable_event
class LogStartEvent(SerializableLogEvent):
    ...


@serializable_event
class LogEndEvent(SerializableLogEvent):
    ...


@serializable_event
class LogWriteEvent(SerializableLogEvent):
    severity: Severity
    message: str


@serializable_event
class UnknownLogEvent(SerializableLogEvent):
    content: str


class LogWriter:
    __slots__ = ('bus', 'name')

    def __init__(self, name: str, bus: Optional[EventBus[SerializableLogEvent]]) -> None:
        self.name = name
        self.bus = bus
        if self.bus:
            self.bus.put(LogStartEvent(name=self.name))

    def emit_log_write_event(self, message: str, severity: Severity) -> None:
        if self.bus:
            self.bus.put(LogWriteEvent(
                name=self.name,
                severity=severity,
                message=message
            ))

    def close(self) -> None:
        if self.bus:
            self.bus.put(LogEndEvent(name=self.name))


class StdOutLogWriterWrapper:
    __slots__ = ('_base')
    _sev = Severity.INFO

    def __init__(self, log_writer: LogWriter) -> None:
        self._base = log_writer

    def write(self, m: str) -> None:
        if m.strip():
            self._base.emit_log_write_event(m, self._sev)

    def writelines(self, mlist: Iterable[str]) -> None:
        for m in mlist:
            self.write(m)

    def flush(self) -> None:
        # Need to imitate file-like object for redirecting stdout
        pass

    def close(self) -> None:
        # Need to imitate file-like object for redirecting stdout
        pass


class StdErrLogWriterWrapper(StdOutLogWriterWrapper):
    _sev = Severity.ERROR


class TaskLogWriterWrapper:
    __slots__ = ('_base')

    def __init__(self, log_writer: LogWriter) -> None:
        self._base = log_writer

    def debug(self, m: str) -> None:
        self._base.emit_log_write_event(m, Severity.DEBUG)

    def info(self, m: str) -> None:
        self._base.emit_log_write_event(m, Severity.INFO)

    def warning(self, m: str) -> None:
        self._base.emit_log_write_event(m, Severity.WARNING)

    def error(self, m: str) -> None:
        self._base.emit_log_write_event(m, Severity.ERROR)

    def critical(self, m: str) -> None:
        self._base.emit_log_write_event(m, Severity.CRITICAL)
