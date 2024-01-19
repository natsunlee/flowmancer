from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from pydantic import BaseModel, Extra

from .lifecycle import Lifecycle

_task_classes = dict()


def task(t: type[Task]):
    if not issubclass(t, Task):
        raise TypeError(f'Must extend `Task` type: {t.__name__}')
    _task_classes[t.__name__] = t
    return t


class Task(ABC, BaseModel, Lifecycle):
    _shared_dict: Dict[str, Any] = dict()

    @property
    def shared_dict(self) -> Dict[str, Any]:
        return self._shared_dict

    # def __init__(self, **kwargs) -> None:
    #     self._shared_dict = kwargs.pop('_shared_dict', None) or dict()  # In case the param is given with None value.
    #     super().__init__(**kwargs)

    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True

    @abstractmethod
    def run(self) -> None:
        pass
