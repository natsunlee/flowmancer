from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any

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
    kwargs: Optional[Dict[str, Any]] = dict()

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