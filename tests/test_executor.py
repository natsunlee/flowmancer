import asyncio
from multiprocessing import Manager

import pytest

from fmancer.events import ExecutionStateTransition, SerializableEvent
from fmancer.executor import ExecutionState, Executor


@pytest.fixture(scope="module")
def manager():
    return Manager()


@pytest.mark.asyncio
async def test_simple_executor_str_class_name(manager):
    q = manager.Queue()
    ex = Executor(name="test", task_class="TestTask", log_queue=q, semaphore=asyncio.Semaphore(1))
    await ex.start()
    msg = q.get()
    assert msg == "hello"


@pytest.mark.asyncio
async def test_simple_executor_class_type(manager, test_task_cls):
    q = manager.Queue()
    ex = Executor(name="test", task_class=test_task_cls, log_queue=q, semaphore=asyncio.Semaphore(1))
    await ex.start()
    msg = q.get()
    assert msg == "hello"


@pytest.mark.asyncio
async def test_repeated_fail(manager):
    q = manager.Queue()
    ex = Executor(name="test", task_class="FailTask", log_queue=q, max_attempts=3)
    await ex.start()
    result = []
    while not q.empty():
        val = q.get().strip()
        if val == "fail":
            result.append(val)
    assert result == ["fail"] * 3


def test_state_transition(manager):
    tname = "TestTask"
    q = manager.Queue()
    ex = Executor(name="test", task_class=tname, event_queue=q)
    ex.state = ExecutionState.PENDING
    ex.state = ExecutionState.RUNNING
    ex.state = ExecutionState.COMPLETED
    events = []
    while not q.empty():
        e = SerializableEvent.deserialize(q.get())
        events.append(e)
    expected = [
        ExecutionStateTransition(name="test", from_state=ExecutionState.INIT, to_state=ExecutionState.PENDING),
        ExecutionStateTransition(name="test", from_state=ExecutionState.PENDING, to_state=ExecutionState.RUNNING),
        ExecutionStateTransition(name="test", from_state=ExecutionState.RUNNING, to_state=ExecutionState.COMPLETED),
    ]
    assert events == expected and ex.state == ExecutionState.COMPLETED


@pytest.mark.asyncio
async def test_state_transition_from_run(manager):
    tname = "TestTask"
    q = manager.Queue()
    ex = Executor(name="test", task_class=tname, event_queue=q)
    await ex.start()
    events = []
    while not q.empty():
        e = SerializableEvent.deserialize(q.get())
        events.append(e)
    expected = [
        ExecutionStateTransition(name="test", from_state=ExecutionState.INIT, to_state=ExecutionState.PENDING),
        ExecutionStateTransition(name="test", from_state=ExecutionState.PENDING, to_state=ExecutionState.RUNNING),
        ExecutionStateTransition(name="test", from_state=ExecutionState.RUNNING, to_state=ExecutionState.COMPLETED),
    ]
    assert events == expected and ex.state == ExecutionState.COMPLETED


@pytest.mark.asyncio
async def test_failed_dependency():
    async def f():
        return False

    ex = Executor(name="test", task_class="TestTask", await_dependencies=f)
    await ex.start()
    assert ex.state == ExecutionState.DEFAULTED


@pytest.mark.asyncio
async def test_succeeded_dependency():
    async def f():
        return True

    ex = Executor(name="test", task_class="TestTask", await_dependencies=f)
    await ex.start()
    assert ex.state == ExecutionState.COMPLETED
