import pytest
from flowmancer.executors.executor import Executor
from flowmancer.tasks.task import Task

class TestExecutor(Executor):
    async def execute(self, task: Task) -> None:
        task.run_lifecycle()

def test_abstract_init_error():
    with pytest.raises(TypeError):
        Executor()