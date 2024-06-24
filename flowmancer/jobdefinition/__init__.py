from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field

_job_definition_classes = dict()
T = TypeVar('T', bound='SerializableJobDefinition')


def job_definition(key: str) -> Callable:
    def inner(t: type[T]) -> Type[T]:
        if not issubclass(t, SerializableJobDefinition):
            raise TypeError(f'Must extend `SerializableJobDefinition` type: {t.__name__}')
        _job_definition_classes[key] = t
        return t
    return inner


class JobDefinitionComponent(BaseModel):
    model_config = ConfigDict(extra='forbid', use_enum_values=True, populate_by_name=True)


class LoggerDefinition(JobDefinitionComponent):
    variant: str = Field(alias='logger')
    parameters: Dict[str, Any] = dict()


class TaskDefinition(JobDefinitionComponent):
    variant: str = Field(alias='task')
    depends_on: List[str] = Field(alias='dependencies', default_factory=list)
    max_attempts: int = 1
    backoff: int = 0
    parameters: Dict[str, Any] = dict()


class ExtensionDefinition(JobDefinitionComponent):
    variant: str = Field(alias='extension')
    parameters: Dict[str, Any] = dict()


class ConfigurationDefinition(JobDefinitionComponent):
    name: str = 'flowmancer'
    max_concurrency: int = 0
    extension_directories: List[str] = []
    extension_packages: List[str] = []
    synchro_interval_seconds: float = 0.25
    loggers_interval_seconds: float = 0.25
    extensions_interval_seconds: float = 0.25
    checkpointer_interval_seconds: float = 10.0


class CheckpointerDefinition(JobDefinitionComponent):
    variant: str = Field(alias='checkpointer')
    parameters: Dict[str, Any] = dict()


class JobDefinition(JobDefinitionComponent):
    version: float = 0.1
    include: List[Path] = []
    config: ConfigurationDefinition = ConfigurationDefinition()
    tasks: Dict[str, TaskDefinition]
    loggers: Dict[str, LoggerDefinition] = {'file-logger': LoggerDefinition(variant='FileLogger')}  # type: ignore
    extensions: Dict[str, ExtensionDefinition] = {
        'progress-bar': ExtensionDefinition(variant='RichProgressBar')  # type: ignore
    }
    checkpointer: CheckpointerDefinition = CheckpointerDefinition(variant='FileCheckpointer')  # type: ignore


class LoadParams(BaseModel):
    model_config = ConfigDict(extra='forbid')
    APP_ROOT_DIR: str = '.'


class SerializableJobDefinition(ABC):
    @abstractmethod
    def load(
        self, filename: Union[Path, str], params: LoadParams = LoadParams(), vars: Optional[Dict[str, str]] = None
    ) -> JobDefinition:
        pass

    @abstractmethod
    def dump(self, jdef: JobDefinition, filename: Union[Path, str]) -> None:
        pass
