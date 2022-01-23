from pydantic import BaseModel
from typing import List, Dict, Optional, Union

class LoggerDefinition(BaseModel):
    module: str
    logger: str
    kwargs: Optional[Dict[str, Union[int, str]]] = dict()

class TaskDefinition(BaseModel):
    module: str
    task: str
    dependencies: Optional[List[str]] = []
    max_attempts: Optional[int] = 1
    backoff: Optional[int] = 0
    args: Optional[List[Union[int, str]]] = []
    kwargs: Optional[Dict[str, Union[int, str]]] = dict()

class ObserverDefinition(BaseModel):
    module: str
    observer: str
    kwargs: Optional[Dict[str, Union[int, str]]] = dict()

class JobDefinition(BaseModel):
    version: float
    name: str
    concurrency: Optional[int]
    tasks: Dict[str, TaskDefinition]
    pypath: Optional[List[str]] = []
    loggers: Optional[Dict[str, LoggerDefinition]] = dict()
    observers: Optional[Dict[str, ObserverDefinition]] = {
        "progressbar": ObserverDefinition(
            module="flowmancer.observers.progressbar",
            observer="ProgressBar"
        )
    }