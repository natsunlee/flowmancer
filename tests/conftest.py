import pytest

from fmancer.task import Task, task


@task
class TestTask(Task):
    def run(self) -> None:
        print("hello")


@pytest.fixture(scope="session")
def test_task_cls():
    return TestTask


@task
class SuccessTask(Task):
    def run(self) -> None:
        print('success')


@pytest.fixture(scope="session")
def success_task_cls():
    return SuccessTask


@task
class FailTask(Task):
    def run(self) -> None:
        raise RuntimeError("fail")


@pytest.fixture(scope="session")
def fail_task_cls():
    return FailTask


@task
class LifecycleSuccessTask(Task):
    def on_create(self) -> None:
        print("on_create")

    def run(self) -> None:
        print("run")

    def on_failure(self) -> None:
        print("on_failure")

    def on_success(self) -> None:
        print("on_success")

    def on_destroy(self) -> None:
        print("on_destroy")


@pytest.fixture(scope="session")
def lifecycle_success_task_cls():
    return LifecycleSuccessTask


@task
class LifecycleFailTask(LifecycleSuccessTask):
    def run(self) -> None:
        raise RuntimeError("Failing!")


@pytest.fixture(scope="session")
def lifecycle_fail_task_cls():
    return LifecycleFailTask
