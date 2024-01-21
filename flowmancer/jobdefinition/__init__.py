from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Type, Union

from pydantic import BaseModel, Extra

_job_definition_classes = dict()


def job_definition(key: str) -> Callable:
    def inner(t: type[SerializableJobDefinition]) -> Type[SerializableJobDefinition]:
        if not issubclass(t, SerializableJobDefinition):
            raise TypeError(f'Must extend `SerializableJobDefinition` type: {t.__name__}')
        _job_definition_classes[key] = t
        return t
    return inner


class JobDefinitionComponent(BaseModel):
    class Config:
        extra = Extra.forbid
        underscore_attrs_are_private = True


class LoggerDefinition(JobDefinitionComponent):
    logger: str
    kwargs: Dict[str, Union[int, str]] = dict()


class TaskDefinition(JobDefinitionComponent):
    task: str
    dependencies: List[str] = []
    max_attempts: int = 1
    backoff: int = 0
    kwargs: Dict[str, Union[int, str]] = dict()


class PluginDefinition(JobDefinitionComponent):
    plugin: str
    kwargs: Dict[str, Union[int, str]] = dict()


class Configuration(JobDefinitionComponent):
    name: str = 'flowmancer'
    concurrency: int = 0
    extension_directories: List[str] = []


class JobDefinition(JobDefinitionComponent):
    version: float = 0.1
    config: Configuration = Configuration()
    tasks: Dict[str, TaskDefinition]
    loggers: Dict[str, LoggerDefinition] = dict()
    plugins: Dict[str, PluginDefinition] = dict()


class SerializableJobDefinition(ABC):
    @abstractmethod
    def load(self, filename: str) -> JobDefinition:
        pass

    @abstractmethod
    def dump(self, jdef: JobDefinition, filename: str) -> None:
        pass
