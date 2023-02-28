from multiprocessing import Manager
from typing import Any, Dict

import pytest

from flowmancer.task import Task, _task_classes, task


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


def test_task_lifecycle_order_success(lifecycle_success_task_cls):
    d: Dict[str, Any] = dict()
    lifecycle_success_task_cls("success", shared_dict=d).run_lifecycle()
    assert d["events"] == ["on_create", "run", "on_success", "on_destroy"]


def test_task_lifecycle_order_fail(lifecycle_fail_task_cls):
    d: Dict[str, Any] = dict()
    lifecycle_fail_task_cls("failure", shared_dict=d).run_lifecycle()
    assert d["events"] == ["on_create", "Failing!", "on_failure", "on_destroy"]
