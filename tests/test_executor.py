from multiprocessing import Process
from typing import Any, Dict, List, Tuple, cast

import pytest

from flowmancer.eventbus import EventBus
from flowmancer.eventbus.execution import (ExecutionState,
                                           ExecutionStateTransition,
                                           SerializableExecutionEvent)
from flowmancer.eventbus.log import LogWriteEvent, SerializableLogEvent
from flowmancer.executor import Executor, ProcessResult, exec_task_lifecycle
from flowmancer.task import _task_classes


@pytest.mark.parametrize('c, is_failed', [
    ('FailTask', True),
    ('SuccessTask', False)
])
def test_exec_task_lifecycle_retcode(c: str, is_failed: bool):
    TaskClass = _task_classes[c]
    result = ProcessResult()
    exec_task_lifecycle(c, TaskClass(), None, result, dict())
    assert(result.is_failed == is_failed)


@pytest.mark.parametrize('c, is_failed', [
    ('FailTask', True),
    ('SuccessTask', False)
])
def test_exec_task_lifecycle_retcode_multiprocess(c: str, is_failed: bool):
    TaskClass = _task_classes[c]
    result = ProcessResult()
    proc = Process(
        target=exec_task_lifecycle,
        args=(c, TaskClass(), None, result),
        daemon=False
    )
    proc.start()
    proc.join()
    assert(result.is_failed == is_failed)


def test_exec_task_lifecycle_order_success():
    LifecycleSuccessTask = _task_classes['LifecycleSuccessTask']
    d: Dict[str, Any] = {'events': []}
    result = ProcessResult()
    exec_task_lifecycle('Test', LifecycleSuccessTask(), None, result, d)
    assert(d['events'] == ['on_create', 'run', 'on_success', 'on_destroy'])


def test_exec_task_lifecycle_order_fail(manager):
    LifecycleFailTask = _task_classes['LifecycleFailTask']
    d: Dict[str, Any] = {'events': []}
    result = ProcessResult()
    exec_task_lifecycle('Test', LifecycleFailTask(), None, result, d)
    assert(d['events'] == ['on_create', 'Failing!', 'on_failure', 'on_destroy'])


def test_executor_state_change_execution_event_bus():
    name = 'test'
    bus = EventBus[SerializableExecutionEvent]()
    ex = Executor(name, 'SuccessTask', None, bus)
    ex.state = ExecutionState.PENDING
    ex.state = ExecutionState.RUNNING
    ex.state = ExecutionState.COMPLETED

    bus_contents = []
    expected = [
        (ExecutionState.INIT.value, ExecutionState.PENDING.value),
        (ExecutionState.PENDING.value, ExecutionState.RUNNING.value),
        (ExecutionState.RUNNING.value, ExecutionState.COMPLETED.value)
    ]
    while not bus.empty():
        t = cast(ExecutionStateTransition, bus.get())
        bus_contents.append((t.from_state, t.to_state))
    assert(bus_contents == expected)


@pytest.mark.parametrize('c, expected', [
    ('SuccessTask', [
        (ExecutionState.INIT.value, ExecutionState.PENDING.value),
        (ExecutionState.PENDING.value, ExecutionState.RUNNING.value),
        (ExecutionState.RUNNING.value, ExecutionState.COMPLETED.value)
    ]),
    ('FailTask', [
        (ExecutionState.INIT.value, ExecutionState.PENDING.value),
        (ExecutionState.PENDING.value, ExecutionState.RUNNING.value),
        (ExecutionState.RUNNING.value, ExecutionState.PENDING.value),
        (ExecutionState.PENDING.value, ExecutionState.RUNNING.value),
        (ExecutionState.RUNNING.value, ExecutionState.FAILED.value)
    ])
])
@pytest.mark.asyncio
async def test_executor_start_execution_event_bus(c: str, expected: List[Tuple[str]]):
    name = 'Test'
    bus = EventBus[SerializableExecutionEvent]()
    ex = Executor(name, c, None, bus, max_attempts=2)
    await ex.start()
    bus_contents = []
    while not bus.empty():
        t = cast(ExecutionStateTransition, bus.get())
        bus_contents.append((t.from_state, t.to_state))
    assert(bus_contents == expected)


@pytest.mark.asyncio
async def test_executor_failed_dependency():
    async def f():
        return False

    ex = Executor('Test', 'SuccessTask', None, None, await_dependencies=f)
    await ex.start()
    assert(ex.state == ExecutionState.DEFAULTED)


@pytest.mark.asyncio
async def test_succeeded_dependency():
    async def f():
        return True

    ex = Executor('Test', 'SuccessTask', None, None, await_dependencies=f)
    await ex.start()
    assert(ex.state == ExecutionState.COMPLETED)


def test_exec_task_lifecycle_shared_dict(manager):
    TestTask = _task_classes['TestTask']
    shared_dict = manager.dict()
    result = ProcessResult()
    exec_task_lifecycle('Test', TestTask(), None, result, shared_dict)
    assert(shared_dict['myvar'] == 'hello')


@pytest.mark.asyncio
async def test_executor_process_shared_dict(manager):
    TestTask = _task_classes['TestTask']
    shared_dict = manager.dict()
    bus = EventBus[SerializableExecutionEvent]()
    ex = Executor('Test', TestTask, None, bus, shared_dict=shared_dict)
    await ex.start()
    assert(shared_dict['myvar'] == 'hello')


def test_task_log_queue(manager):
    LifecycleSuccessTask = _task_classes['LifecycleSuccessTask']
    bus = EventBus[SerializableLogEvent](manager.Queue())
    shared_dict = manager.dict()
    shared_dict['events'] = []
    result = ProcessResult()
    proc = Process(
        target=exec_task_lifecycle,
        args=('Test', LifecycleSuccessTask(), bus, result, shared_dict),
        daemon=False
    )
    proc.start()
    proc.join()
    log_result = []
    while not bus.empty():
        msg = bus.get()
        if isinstance(msg, LogWriteEvent) and msg.message != '\n':
            log_result.append(msg.message)
    print(log_result)
    assert(log_result == ['on_create', 'run', 'on_success', 'on_destroy'])
