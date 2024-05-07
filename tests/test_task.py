from multiprocessing import Manager
from typing import Any, Dict, cast

import pytest

from flowmancer.task import Task, _task_classes, task


def test_task_deco():
    @task
    class Something(Task):
        def run(self) -> None:
            return

    assert _task_classes['Something'] == Something


def test_task_deco_exception():
    with pytest.raises(TypeError):
        @task  # type: ignore
        class Improper:
            def run(self) -> None:
                return


def test_task_shared_dict():
    m = Manager()
    shared_dict = m.dict()
    task_instance = _task_classes['TestTask'](shared_dict=cast(Dict[str, Any], shared_dict))
    task_instance.run()
    assert(shared_dict['myvar'] == 'hello')
