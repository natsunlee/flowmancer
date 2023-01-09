from typing import Dict, List, Optional, Union

from pydantic import BaseModel


class LoggerDefinition(BaseModel):
    module: str
    logger: str
    kwargs: Dict[str, Union[int, str]] = dict()


class TaskDefinition(BaseModel):
    module: str
    task: str
    dependencies: List[str] = []
    max_attempts: int = 1
    backoff: int = 0
    args: List[Union[int, str]] = []
    kwargs: Dict[str, Union[int, str]] = dict()


class ObserverDefinition(BaseModel):
    module: str
    observer: str
    kwargs: Dict[str, Union[int, str]] = dict()


class JobDefinition(BaseModel):
    version: float
    name: str
    concurrency: Optional[int]
    tasks: Dict[str, TaskDefinition]
    pypath: List[str] = []
    loggers: Dict[str, LoggerDefinition] = dict()
    observers: Dict[str, ObserverDefinition] = {
        "progressbar": ObserverDefinition(module="flowmancer.observers.progressbar", observer="ProgressBar")
    }
