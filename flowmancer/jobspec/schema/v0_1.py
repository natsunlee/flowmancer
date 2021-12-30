from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class FileLoggerDefinition(BaseModel):
    path: str

class LoggersDefinition(BaseModel):
    file: Optional[FileLoggerDefinition]

class TaskDefinition(BaseModel):
    module: str
    task: str
    dependencies: Optional[List[str]]
    max_attempts: Optional[int]
    backoff: Optional[int]
    kwargs: Optional[Dict[str, Any]]

class SnapshotsDefinition(BaseModel):
    path: str

class JobDefinition(BaseModel):
    version: float
    name: str
    pypath: List[str]
    loggers: LoggersDefinition
    tasks: Dict[str, TaskDefinition]
    snapshots: Optional[SnapshotsDefinition]