from __future__ import annotations

from abc import ABC, abstractmethod

from ..executor import SerializableExecutionEvent
from ..lifecycle import AsyncLifecycle

_observer_classes = dict()


def observer(t: type[Observer]):
    if not issubclass(t, Observer):
        raise TypeError(f'Must extend `Observer` type: {t.__name__}')
    _observer_classes[t.__name__] = t
    return t


class Observer(ABC, AsyncLifecycle):
    @abstractmethod
    async def update(self, e: SerializableExecutionEvent) -> None:
        pass
