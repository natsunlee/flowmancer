from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, Extra

from ..executor import SerializableExecutionEvent
from ..lifecycle import AsyncLifecycle

_plugin_classes = dict()


def plugin(t: type[Plugin]):
    if not issubclass(t, Plugin):
        raise TypeError(f'Must extend `Plugin` type: {t.__name__}')
    _plugin_classes[t.__name__] = t
    return t


class Plugin(ABC, AsyncLifecycle, BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True

    @abstractmethod
    async def update(self, e: SerializableExecutionEvent) -> None:
        pass
