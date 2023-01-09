import pytest

from flowmancer.executors.executor import Executor


def test_abstract_init_error():
    with pytest.raises(TypeError):
        Executor()  # type: ignore
