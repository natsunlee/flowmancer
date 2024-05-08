from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict

from ..executor import SerializableExecutionEvent
from ..lifecycle import AsyncLifecycle

_extension_classes = dict()


def extension(t: type[Extension]):
    if not issubclass(t, Extension):
        raise TypeError(f'Must extend `Extension` type: {t.__name__}')
    _extension_classes[t.__name__] = t
    return t


class Extension(ABC, AsyncLifecycle, BaseModel):
    model_config = ConfigDict(extra='forbid')

    @abstractmethod
    async def update(self, e: SerializableExecutionEvent) -> None:
        pass
