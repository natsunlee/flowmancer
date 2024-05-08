from datetime import datetime
from typing import cast

from flowmancer.eventbus import EventBus
from flowmancer.eventbus.log import LogEndEvent, LogStartEvent, LogWriteEvent, LogWriter, SerializableLogEvent, Severity


def test_enum_is_preserved():
    bus = EventBus[SerializableLogEvent]()
    bus.put(LogWriteEvent(
        name='test',
        severity=Severity.INFO,
        message='ok',
        timestamp=datetime.now()
    ))
    e = cast(LogWriteEvent, bus.get())
    assert isinstance(e.severity, Severity)
    assert e.severity == Severity.INFO


def test_timestamp_is_preserved():
    ts = datetime(year=2024, month=2, day=11, hour=13, minute=48, second=1)
    bus = EventBus[SerializableLogEvent]()
    bus.put(LogWriteEvent(
        name='test',
        severity=Severity.INFO,
        message='ok',
        timestamp=ts
    ))
    e = cast(LogWriteEvent, bus.get())
    assert isinstance(e.timestamp, datetime)
    assert e.timestamp == ts


def test_log_event_order():
    bus = EventBus[SerializableLogEvent]()
    writer = LogWriter('test', bus)
    writer.emit_log_write_event('hello world', Severity.DEBUG)
    writer.close()
    assert isinstance(bus.get(), LogStartEvent)
    assert isinstance(bus.get(), LogWriteEvent)
    assert isinstance(bus.get(), LogEndEvent)
