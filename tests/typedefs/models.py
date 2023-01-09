import pytest
from pydantic.error_wrappers import ValidationError

from flowmancer.typedefs.models import LoggerDefinition


@pytest.mark.parametrize(
    'kwargs',
    [
        {"module": "val", "logger": "val"},
        {"module": "my_module", "logger": "my_logger", "kwargs": {"one": 1, "two": "2two"}},
    ],
)
def test_logger_definition_assignments(kwargs):
    x = LoggerDefinition(**kwargs)
    assert (x.module, x.logger, x.kwargs) == (
        kwargs["module"],
        kwargs["logger"],
        kwargs.get("kwargs") if kwargs.get("kwargs") else dict(),
    )


@pytest.mark.parametrize(
    'kwargs',
    [
        {"module": "val"},
        {"logger": "val"},
        {"module": "val", "logger": None},
        {"module": None, "logger": "val"},
        {"kwargs": {"one": 1, "two": "2two"}},
    ],
)
def test_logger_definition_missing_required(kwargs):
    with pytest.raises(ValidationError):
        LoggerDefinition(**kwargs)
