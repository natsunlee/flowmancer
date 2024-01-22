import asyncio

import pytest

from flowmancer.eventbus.execution import ExecutionState, ExecutionStateTransition
from flowmancer.eventbus.log import LogWriteEvent, Severity
from flowmancer.flowmancer import Flowmancer


# ADD EXECUTOR TESTS
def test_add_executor(test_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=test_task_cls)
    assert (
        len(f._executors) == 1
        and isinstance(f._executors['a'].instance.get_task_instance(), test_task_cls)
        and not f._executors['a'].dependencies
        and len(f._states[ExecutionState.INIT]) == 1
        and 'a' in f._states[ExecutionState.INIT]
    )


def test_add_executor_by_str_name(test_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class='TestTask')
    assert (
        len(f._executors) == 1
        and isinstance(f._executors['a'].instance.get_task_instance(), test_task_cls)
        and not f._executors['a'].dependencies
        and len(f._states[ExecutionState.INIT]) == 1
        and 'a' in f._states[ExecutionState.INIT]
    )


def test_add_executors_with_deps(test_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=test_task_cls)
    f.add_executor(name='b', task_class=test_task_cls)
    f.add_executor(name='c', task_class=test_task_cls, deps=['a', 'b'])
    f.add_executor(name='d', task_class=test_task_cls, deps=['c'])
    assert (
        len(f._executors) == 4
        and not f._executors['a'].dependencies
        and not f._executors['b'].dependencies
        and f._executors['c'].dependencies == ['a', 'b']
        and f._executors['d'].dependencies == ['c']
        and len(f._states[ExecutionState.INIT]) == 4
        and {'a', 'b', 'c', 'd'} == f._states[ExecutionState.INIT]
    )


def test_successful_deps_validation(test_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=test_task_cls)
    f.add_executor(name='b', task_class=test_task_cls)
    f.add_executor(name='c', task_class=test_task_cls, deps=['a', 'b'])
    f.add_executor(name='d', task_class=test_task_cls, deps=['c'])
    assert f._dependencies_are_valid()


def test_failed_deps_validation_missing_dep(test_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=test_task_cls, deps=['x'])
    assert not f._dependencies_are_valid()


def test_failed_deps_validation_self_ref(test_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=test_task_cls, deps=['a'])
    assert not f._dependencies_are_valid()


@pytest.mark.asyncio
async def test_synchro_ends():
    root_event = asyncio.Event()
    f = Flowmancer(test=True)
    f._states[ExecutionState.INIT].add('test')
    tasks = f._init_executors(root_event)
    root_event.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_log_pusher_ends_root_event():
    root_event = asyncio.Event()
    f = Flowmancer(test=True)
    tasks = f._init_loggers(root_event)
    root_event.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_log_pusher_ends_empty_queue():
    root_event = asyncio.Event()
    f = Flowmancer(test=True)
    f._log_event_bus.put(LogWriteEvent(name='test', severity=Severity.INFO, message='test'))
    tasks = f._init_loggers(root_event)
    root_event.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_observer_pusher_ends_root_event():
    root_event = asyncio.Event()
    f = Flowmancer(test=True)
    tasks = f._init_extensions(root_event)
    root_event.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_observer_pusher_ends_empty_queue(success_task_cls):
    root_event = asyncio.Event()
    f = Flowmancer(test=True)
    f.add_executor(name='test', task_class=success_task_cls)
    f._execution_event_bus.put(
        ExecutionStateTransition(
            name='test',
            from_state=ExecutionState.INIT,
            to_state=ExecutionState.PENDING
        )
    )
    tasks = f._init_extensions(root_event)
    root_event.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_all_pusher_ends_empty_queue(success_task_cls):
    root_event = asyncio.Event()
    f = Flowmancer(test=True)
    f.add_executor(name='test', task_class=success_task_cls)
    f._log_event_bus.put(LogWriteEvent(name='test', severity=Severity.INFO, message='test'))
    f._execution_event_bus.put(
        ExecutionStateTransition(
            name='test',
            from_state=ExecutionState.INIT,
            to_state=ExecutionState.PENDING
        )
    )
    tasks = f._init_extensions(root_event) + f._init_loggers(root_event)
    root_event.set()
    await asyncio.gather(*tasks)


# EXECUTOR RUN TESTS
@pytest.mark.asyncio
async def test_single_executor_success_run_initiate(success_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=success_task_cls)
    retcode = await f._initiate()
    assert (
        not f._states[ExecutionState.PENDING]
        and len(f._states[ExecutionState.COMPLETED]) == 1
        and f._executors['a'].instance.state == ExecutionState.COMPLETED
        and retcode == 0
    )


def test_single_executor_success_run(success_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=success_task_cls)
    retcode = f.start()
    assert (
        not f._states[ExecutionState.PENDING]
        and len(f._states[ExecutionState.COMPLETED]) == 1
        and f._executors['a'].instance.state == ExecutionState.COMPLETED
        and retcode == 0
    )


def test_multiple_executor_success_run(success_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=success_task_cls)
    f.add_executor(name='b', task_class=success_task_cls)
    f.add_executor(name='c', task_class=success_task_cls, deps=['a', 'b'])
    f.add_executor(name='d', task_class=success_task_cls, deps=['c'])
    retcode = f.start()
    assert (
        not f._states[ExecutionState.PENDING]
        and len(f._states[ExecutionState.COMPLETED]) == 4
        and f._executors['a'].instance.state == ExecutionState.COMPLETED
        and f._executors['b'].instance.state == ExecutionState.COMPLETED
        and f._executors['c'].instance.state == ExecutionState.COMPLETED
        and f._executors['d'].instance.state == ExecutionState.COMPLETED
        and retcode == 0
    )


def test_multiple_executor_fail_run(success_task_cls, fail_task_cls):
    f = Flowmancer(test=True)
    f.add_executor(name='a', task_class=success_task_cls)
    f.add_executor(name='b', task_class=fail_task_cls)
    f.add_executor(name='c', task_class=success_task_cls, deps=['a', 'b'])
    f.add_executor(name='d', task_class=success_task_cls, deps=['a'])
    retcode = f.start()
    assert (
        not f._states[ExecutionState.PENDING]
        and len(f._states[ExecutionState.COMPLETED]) == 2
        and len(f._states[ExecutionState.FAILED]) == 1
        and len(f._states[ExecutionState.DEFAULTED]) == 1
        and f._executors['a'].instance.state == ExecutionState.COMPLETED
        and f._executors['b'].instance.state == ExecutionState.FAILED
        and f._executors['c'].instance.state == ExecutionState.DEFAULTED
        and f._executors['d'].instance.state == ExecutionState.COMPLETED
        and retcode == 2
    )
