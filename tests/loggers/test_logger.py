import pytest
from flowmancer.loggers.logger import Logger

def test_abstract_init_error():
    with pytest.raises(TypeError):
        Logger()