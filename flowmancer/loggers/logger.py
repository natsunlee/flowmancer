from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar

from pydantic import BaseModel, ConfigDict

from ..eventbus.log import SerializableLogEvent
from ..lifecycle import AsyncLifecycle

_logger_classes = dict()
T = TypeVar('T', bound='Logger')


def logger(t: type[T]) -> type[T]:
    if not issubclass(t, Logger):
        raise TypeError(f'Must extend `Logger` type: {t.__name__}')
    _logger_classes[t.__name__] = t
    return t


class Logger(ABC, AsyncLifecycle, BaseModel):
    model_config = ConfigDict(extra='forbid')

    @abstractmethod
    async def update(self, m: SerializableLogEvent) -> None:
        pass
