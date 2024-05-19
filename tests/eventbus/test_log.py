from datetime import datetime
from typing import cast

from flowmancer.eventbus import EventBus, SerializableEvent
from flowmancer.eventbus.log import LogEndEvent, LogStartEvent, LogWriteEvent, LogWriter, SerializableLogEvent, Severity


def test_enum_is_preserved():
    bus = EventBus[SerializableLogEvent]('flowmancer')
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
    bus = EventBus[SerializableLogEvent]('flowmancer')
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
    bus = EventBus[SerializableLogEvent]('flowmancer')
    writer = LogWriter('test', bus)
    writer.emit_log_write_event('hello world', Severity.DEBUG)
    writer.close()
    assert isinstance(bus.get(), LogStartEvent)
    assert isinstance(bus.get(), LogWriteEvent)
    assert isinstance(bus.get(), LogEndEvent)


def test_job_name_not_injected_on_put():
    bus = EventBus[SerializableLogEvent]('custom-job-name')
    writer = LogWriter('test', bus)
    writer.emit_log_write_event('hello world', Severity.DEBUG)
    writer.close()
    q = bus._queue
    assert SerializableEvent.deserialize(q.get()).job_name is None
    assert SerializableEvent.deserialize(q.get()).job_name is None
    assert SerializableEvent.deserialize(q.get()).job_name is None


def test_job_name_injected_on_get():
    bus = EventBus[SerializableLogEvent]('custom-job-name')
    writer = LogWriter('test', bus)
    writer.emit_log_write_event('hello world', Severity.DEBUG)
    writer.close()
    assert bus.get().job_name == 'custom-job-name'
    assert bus.get().job_name == 'custom-job-name'
    assert bus.get().job_name == 'custom-job-name'
