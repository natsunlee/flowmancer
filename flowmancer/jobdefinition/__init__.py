from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Type, Union

from pydantic import BaseModel

_job_definition_classes = dict()


def job_definition(key: str) -> Callable:
    def inner(t: type[SerializableJobDefinition]) -> Type[SerializableJobDefinition]:
        if not issubclass(t, SerializableJobDefinition):
            raise TypeError(f'Must extend `SerializableJobDefinition` type: {t.__name__}')
        _job_definition_classes[key] = t
        return t
    return inner


class LoggerDefinition(BaseModel):
    logger: str
    kwargs: Dict[str, Union[int, str]] = dict()


class TaskDefinition(BaseModel):
    task: str
    dependencies: List[str] = []
    max_attempts: int = 1
    backoff: int = 0
    kwargs: Dict[str, Union[int, str]] = dict()


class ObserverDefinition(BaseModel):
    observer: str
    kwargs: Dict[str, Union[int, str]] = dict()


class JobDefinition(BaseModel):
    concurrency: int = 0
    tasks: Dict[str, TaskDefinition]
    loggers: Dict[str, LoggerDefinition] = dict()
    observers: Dict[str, ObserverDefinition] = dict()


class SerializableJobDefinition(ABC):
    @abstractmethod
    def load(self, filename: str) -> JobDefinition:
        pass

    @abstractmethod
    def dump(self, jdef: JobDefinition, filename: str) -> None:
        pass
