from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class TaskDefinition(BaseModel):
    module: str
    task: str
    dependencies: Optional[List[str]]
    max_attempts: Optional[int]
    backoff: Optional[int]
    kwargs: Optional[Dict[str, Any]]
    logging: Optional[Dict[str, Any]]

class JobDefinition(BaseModel):
    version: float
    pypath: List[str]
    logger: Dict[str, Any]
    tasks: Dict[str, TaskDefinition]