from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, Extra

from ..executor import SerializableExecutionEvent
from ..lifecycle import AsyncLifecycle

_extension_classes = dict()


def extension(t: type[Extension]):
    if not issubclass(t, Extension):
        raise TypeError(f'Must extend `Extension` type: {t.__name__}')
    _extension_classes[t.__name__] = t
    return t


class Extension(ABC, AsyncLifecycle, BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True
        use_enum_values = True

    @abstractmethod
    async def update(self, e: SerializableExecutionEvent) -> None:
        pass
