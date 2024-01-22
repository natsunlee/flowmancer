from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Extra

from ..eventbus.log import SerializableLogEvent
from ..lifecycle import AsyncLifecycle

_logger_classes = dict()


def logger(t: type[Logger]) -> Any:
    if not issubclass(t, Logger):
        raise TypeError(f'Must extend `Logger` type: {t.__name__}')
    _logger_classes[t.__name__] = t
    return t


class Logger(ABC, AsyncLifecycle, BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True
        use_enum_values = True

    @abstractmethod
    async def update(self, m: SerializableLogEvent) -> None:
        pass
