import pytest

from flowmancer.task import Task, task


@task
class TestTask(Task):
    def run(self) -> None:
        self.shared_dict["myvar"] = "hello"
        print("hello")


@pytest.fixture(scope="session")
def test_task_cls():
    return TestTask


@task
class SuccessTask(Task):
    def run(self) -> None:
        self.shared_dict["myvar"] = "success"
        print('success')


@pytest.fixture(scope="session")
def success_task_cls():
    return SuccessTask


@task
class FailTask(Task):
    def run(self) -> None:
        if "fail_counter" not in self.shared_dict:
            self.shared_dict["fail_counter"] = 1
        else:
            self.shared_dict["fail_counter"] += 1
        raise RuntimeError("fail")


@pytest.fixture(scope="session")
def fail_task_cls():
    return FailTask


@task
class LifecycleSuccessTask(Task):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.shared_dict["events"] = []

    def on_create(self) -> None:
        self.shared_dict["events"].append("on_create")
        print("on_create")

    def run(self) -> None:
        self.shared_dict["events"].append("run")
        print("run")

    def on_failure(self) -> None:
        self.shared_dict["events"].append("on_failure")
        print("on_failure")

    def on_success(self) -> None:
        self.shared_dict["events"].append("on_success")
        print("on_success")

    def on_destroy(self) -> None:
        self.shared_dict["events"].append("on_destroy")
        print("on_destroy")


@pytest.fixture(scope="session")
def lifecycle_success_task_cls():
    return LifecycleSuccessTask


@task
class LifecycleFailTask(LifecycleSuccessTask):
    def run(self) -> None:
        self.shared_dict["events"].append("Failing!")
        raise RuntimeError("Failing!")


@pytest.fixture(scope="session")
def lifecycle_fail_task_cls():
    return LifecycleFailTask
