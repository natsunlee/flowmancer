from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import defaultdict
from queue import Queue
from typing import Any, Dict, Generic, Optional, Type, TypeVar

from pydantic import BaseModel, Extra

_event_classes: Dict[str, Dict[str, Type[SerializableEvent]]] = defaultdict(dict)


class NotASerializableEventError(Exception):
    pass


class NotADeserializableEventError(Exception):
    pass


class SerializableEvent(BaseModel, ABC):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True
        use_enum_values = True

    @classmethod
    @abstractmethod
    def event_group(cls) -> str:
        raise AttributeError('The `event_group` method has not been defined.')

    def serialize(self) -> str:
        try:
            return json.dumps({'group': self.event_group(), 'event': type(self).__name__, 'body': self.dict()})
        except Exception as e:
            raise NotASerializableEventError(str(e))

    @classmethod
    def deserialize(cls, e: str) -> Any:
        try:
            data = json.loads(e)
        except Exception as err:
            raise NotADeserializableEventError(str(err))
        if 'group' not in data or 'event' not in data or 'body' not in data:
            return UnknownEvent(content=e)
        else:
            return _event_classes[data['group']][data['event']](**data['body'])


class UnknownEvent(SerializableEvent):
    content: str

    @classmethod
    def event_group(cls) -> str:
        return 'UnknownEvent'


T = TypeVar('T', bound=SerializableEvent)


class EventBus(Generic[T]):
    __slots__ = ('_queue',)

    def __init__(self, q: Optional[Queue[str]] = None) -> None:
        self._queue = q or Queue()

    def put(self, m: T) -> None:
        self._queue.put(m.serialize())

    def get(self) -> T:
        return SerializableEvent.deserialize(self._queue.get())

    def empty(self) -> bool:
        return self._queue.empty()


def serializable_event(t: type[T]) -> type[T]:
    if not issubclass(t, SerializableEvent):
        raise TypeError(f'Must extend `SerializableEvent` type: {t.__name__}')
    _event_classes[t.event_group()][t.__name__] = t
    return t
