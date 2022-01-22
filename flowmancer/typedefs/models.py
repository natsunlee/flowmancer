from pydantic import BaseModel
from typing import List, Dict, Optional, Union

class FileLoggerDefinition(BaseModel):
    path: str
    retention_days: Optional[int] = -1

class LoggersDefinition(BaseModel):
    file: Optional[FileLoggerDefinition]

class TaskDefinition(BaseModel):
    module: str
    task: str
    dependencies: Optional[List[str]] = []
    max_attempts: Optional[int] = 1
    backoff: Optional[int] = 0
    args: Optional[List[Union[str, int]]] = []
    kwargs: Optional[Dict[str, Union[str, int]]] = dict()

class ObserverDefinition(BaseModel):
    module: str
    observer: str
    kwargs: Optional[Dict[str, Union[str, int]]] = dict()

class SnapshotsDefinition(BaseModel):
    path: str

class JobDefinition(BaseModel):
    version: float
    name: str
    concurrency: Optional[int]
    tasks: Dict[str, TaskDefinition]
    pypath: Optional[List[str]] = []
    loggers: Optional[LoggersDefinition] = LoggersDefinition()
    snapshots: Optional[SnapshotsDefinition] = SnapshotsDefinition(path="./.flowmancer")
    observers: Optional[Dict[str, ObserverDefinition]] = {
        "progressbar": ObserverDefinition(
            module="flowmancer.observers.progressbar",
            observer="ProgressBar"
        )
    }