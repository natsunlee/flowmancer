from multiprocessing import Manager

import pytest

from fmancer.task import Task, _task_classes, task


@pytest.fixture(scope="module")
def manager():
    return Manager()


def test_task_deco():
    @task
    class Something(Task):
        def run(self) -> None:
            return

    assert _task_classes["Something"] == Something


def test_task_deco_exception():
    with pytest.raises(TypeError):

        @task  # type: ignore
        class Improper:
            def run(self) -> None:
                return


def test_task_lifecycle_order_success(manager, lifecycle_success_task_cls):
    q = manager.Queue()
    lifecycle_success_task_cls(q).run_lifecycle()
    result = []
    while not q.empty():
        val = q.get().strip()
        if val:
            result.append(val)
    assert result == ["on_create", "run", "on_success", "on_destroy"]


def test_task_lifecycle_order_fail(manager, lifecycle_fail_task_cls):
    q = manager.Queue()
    lifecycle_fail_task_cls(q).run_lifecycle()
    result = []
    while not q.empty():
        val = q.get().strip()
        if val:
            result.append(val)
    assert result == ["on_create", "Failing!", "on_failure", "on_destroy"]
