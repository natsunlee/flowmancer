from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from queue import Queue
from typing import Any, Dict, Generic, Optional, Type, TypeVar

from pydantic import BaseModel, ConfigDict, field_serializer

_event_classes: Dict[str, Dict[str, Type[SerializableEvent]]] = defaultdict(dict)


class NotASerializableEventError(Exception):
    pass


class NotADeserializableEventError(Exception):
    pass


class SerializableEvent(BaseModel, ABC):
    model_config = ConfigDict(extra='forbid')
    job_name: Optional[str] = None
    timestamp: datetime = datetime.now(timezone.utc).astimezone()

    @classmethod
    @abstractmethod
    def event_group(cls) -> str:
        raise AttributeError('The `event_group` method has not been defined.')

    @field_serializer('timestamp')
    def serialize_timestamp(self, timestamp: datetime, *_):
        return timestamp.isoformat()

    def serialize(self) -> str:
        try:
            return json.dumps({'group': self.event_group(), 'event': type(self).__name__, 'body': self.model_dump()})
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
    __slots__ = ('_queue', 'job_name')

    def __init__(self, j: str, q: Optional[Queue[str]] = None) -> None:
        self._queue = q or Queue()
        self.job_name = j

    def put(self, m: T) -> None:
        self._queue.put(m.serialize())

    def get(self) -> T:
        # Until it's better determined how to handle global properties, inject into events as they are read.
        # For now, no checks on key name collisions - not too big an issue since `EventBus` not accepted from users.
        event = SerializableEvent.deserialize(self._queue.get())
        event.job_name = self.job_name
        return event

    def empty(self) -> bool:
        return self._queue.empty()


def serializable_event(t: type[T]) -> type[T]:
    if not issubclass(t, SerializableEvent):
        raise TypeError(f'Must extend `SerializableEvent` type: {t.__name__}')
    _event_classes[t.event_group()][t.__name__] = t
    return t
