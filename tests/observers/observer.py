import pytest
from flowmancer.observers.observer import Observer

def test_abstract_init_error():
    with pytest.raises(TypeError):
        Observer()