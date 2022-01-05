from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class FileLoggerDefinition(BaseModel):
    path: str

class LoggersDefinition(BaseModel):
    file: Optional[FileLoggerDefinition]

class TaskDefinition(BaseModel):
    module: str
    task: str
    dependencies: Optional[List[str]] = []
    max_attempts: Optional[int] = 1
    backoff: Optional[int] = 0
    kwargs: Optional[Dict[str, Any]] = dict()

class SnapshotsDefinition(BaseModel):
    path: str

class JobDefinition(BaseModel):
    version: float
    name: str
    tasks: Dict[str, TaskDefinition]
    pypath: Optional[List[str]] = []
    loggers: Optional[LoggersDefinition] = LoggersDefinition()
    snapshots: Optional[SnapshotsDefinition] = SnapshotsDefinition(path="./.flowmancer")