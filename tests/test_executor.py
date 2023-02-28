import asyncio
from multiprocessing import Manager

import pytest

from flowmancer.executor import (ExecutionState, ExecutionStateTransition,
                                 Executor, SerializableExecutionEvent)


@pytest.fixture(scope="module")
def manager():
    return Manager()


@pytest.mark.asyncio
async def test_simple_executor_str_class_name(manager):
    d = manager.dict()
    ex = Executor(name="test", task_class="TestTask", shared_dict=d, semaphore=asyncio.Semaphore(1))
    await ex.start()
    assert d["myvar"] == "hello"


@pytest.mark.asyncio
async def test_simple_executor_class_type(manager, test_task_cls):
    d = manager.dict()
    ex = Executor(name="test", task_class=test_task_cls, shared_dict=d, semaphore=asyncio.Semaphore(1))
    await ex.start()
    assert d["myvar"] == "hello"


@pytest.mark.asyncio
async def test_repeated_fail(manager):
    d = manager.dict()
    ex = Executor(name="test", task_class="FailTask", shared_dict=d, max_attempts=3)
    await ex.start()
    assert d["fail_counter"] == 3


def test_state_transition(manager):
    tname = "TestTask"
    q = manager.Queue()
    ex = Executor(name="test", task_class=tname, event_queue=q)
    ex.state = ExecutionState.PENDING
    ex.state = ExecutionState.RUNNING
    ex.state = ExecutionState.COMPLETED
    events = []
    while not q.empty():
        e = SerializableExecutionEvent.deserialize(q.get())
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
        e = SerializableExecutionEvent.deserialize(q.get())
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
