from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..eventbus.log import SerializableLogEvent
from ..lifecycle import AsyncLifecycle

_logger_classes = dict()


def logger(t: type[Logger]) -> Any:
    if not issubclass(t, Logger):
        raise TypeError(f'Must extend `Logger` type: {t.__name__}')
    _logger_classes[t.__name__] = t
    return t


class Logger(ABC, AsyncLifecycle):
    @abstractmethod
    async def update(self, m: SerializableLogEvent) -> None:
        pass
