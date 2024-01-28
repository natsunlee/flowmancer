from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Type

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
        use_enum_values = True


class LoggerDefinition(JobDefinitionComponent):
    logger: str
    parameters: Dict[str, Any] = dict()


class TaskDefinition(JobDefinitionComponent):
    task: str
    dependencies: List[str] = []
    max_attempts: int = 1
    backoff: int = 0
    parameters: Dict[str, Any] = dict()


class ExtensionDefinition(JobDefinitionComponent):
    extension: str
    parameters: Dict[str, Any] = dict()


class Configuration(JobDefinitionComponent):
    name: str = 'flowmancer'
    max_concurrency: int = 0
    extension_directories: List[str] = []
    extension_packages: List[str] = []


class CheckpointerDefinition(JobDefinitionComponent):
    checkpointer: str
    parameters: Dict[str, Any] = dict()


class JobDefinition(JobDefinitionComponent):
    version: float = 0.1
    config: Configuration = Configuration()
    tasks: Dict[str, TaskDefinition]
    loggers: Dict[str, LoggerDefinition] = {'file-logger': LoggerDefinition(logger='FileLogger')}
    extensions: Dict[str, ExtensionDefinition] = {'progress-bar': ExtensionDefinition(extension='RichProgressBar')}
    checkpointer_config: CheckpointerDefinition = CheckpointerDefinition(checkpointer='FileCheckpointer')


class SerializableJobDefinition(ABC):
    @abstractmethod
    def load(self, filename: str) -> JobDefinition:
        pass

    @abstractmethod
    def dump(self, jdef: JobDefinition, filename: str) -> None:
        pass
