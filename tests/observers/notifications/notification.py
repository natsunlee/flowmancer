import pytest
from flowmancer.observers.notifications.notification import Notification

def test_abstract_init_error():
    with pytest.raises(TypeError):
        Notification()