from multiprocessing import Manager

import pytest

from flowmancer.task import Task, task


@pytest.fixture(scope='session')
def manager():
    return Manager()


@task
class TestTask(Task):
    def run(self) -> None:
        self.shared_dict['myvar'] = 'hello'
        print('hello')


@pytest.fixture(scope='session')
def test_task_cls():
    return TestTask


@task
class SuccessTask(Task):
    def run(self) -> None:
        self.shared_dict['myvar'] = 'success'
        print('success')


@pytest.fixture(scope='session')
def success_task_cls():
    return SuccessTask


@task
class FailTask(Task):
    def run(self) -> None:
        if 'fail_counter' not in self.shared_dict:
            self.shared_dict['fail_counter'] = 1
        else:
            self.shared_dict['fail_counter'] += 1
        raise RuntimeError('fail')


@pytest.fixture(scope='session')
def fail_task_cls():
    return FailTask


@task
class LifecycleSuccessTask(Task):
    def on_create(self) -> None:
        e = self.shared_dict['events']
        e.append('on_create')
        self.shared_dict['events'] = e
        print('on_create')

    def run(self) -> None:
        e = self.shared_dict['events']
        e.append('run')
        self.shared_dict['events'] = e
        print('run')

    def on_failure(self) -> None:
        e = self.shared_dict['events']
        e.append('on_failure')
        self.shared_dict['events'] = e
        print('on_failure')

    def on_success(self) -> None:
        e = self.shared_dict['events']
        e.append('on_success')
        self.shared_dict['events'] = e
        print('on_success')

    def on_destroy(self) -> None:
        e = self.shared_dict['events']
        e.append('on_destroy')
        self.shared_dict['events'] = e
        print('on_destroy')


@pytest.fixture(scope='session')
def lifecycle_success_task_cls():
    return LifecycleSuccessTask


@task
class LifecycleFailTask(LifecycleSuccessTask):
    def run(self) -> None:
        e = self.shared_dict['events']
        e.append('Failing!')
        self.shared_dict['events'] = e
        raise RuntimeError('Failing!')


@pytest.fixture(scope='session')
def lifecycle_fail_task_cls():
    return LifecycleFailTask


@task
class WriteAllLogTypes(Task):
    def run(self) -> None:
        print('stdout')
        self.logger.info('info')
        self.logger.debug('debug')
        self.logger.warning('warning')
        self.logger.error('error')
        self.logger.critical('critical')
        raise RuntimeError('stderr')
