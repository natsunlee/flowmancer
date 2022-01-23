import pytest
from flowmancer.jobspecs.jobspec import JobSpec

def test_abstract_init_error():
    with pytest.raises(TypeError):
        JobSpec()