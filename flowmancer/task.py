from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, SkipValidation

from .eventbus.log import LogWriter, TaskLogWriterWrapper
from .lifecycle import Lifecycle

_task_classes = dict()


def task(t: type[Task]):
    if not issubclass(t, Task):
        raise TypeError(f'Must extend `Task` type: {t.__name__}')
    _task_classes[t.__name__] = t
    return t


class Task(ABC, BaseModel, Lifecycle):
    model_config = ConfigDict(extra='forbid', arbitrary_types_allowed=True)
    # Need to skip validation for these, which may contain `multiprocessing.managers` objects. Validation appears to
    # interfere with their functioning...
    shared_dict: SkipValidation[Dict[str, Any]] = Field(default_factory=dict, frozen=True)
    logger: SkipValidation[TaskLogWriterWrapper] = Field(
        default=TaskLogWriterWrapper(LogWriter('Task', None)), frozen=True
    )

    @abstractmethod
    def run(self) -> None:
        pass
