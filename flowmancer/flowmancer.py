import asyncio, sys
from typing import Dict
from .executor import Executor
from .jobspec.schema.v0_1 import JobDefinition
from .typedefs.exceptions import ExistingTaskName
from .jobspec.yaml import YAML
from .watchers.progressbar import ProgressBar
from .logger.file import FileLogger
from .logger.logger import Logger

def update_python_path(job: JobDefinition) -> None:
    for p in (job.get("pypath") or []):
        if p not in sys.path:
            sys.path.append(p)

def read_jobspec(filename: str) -> None:
    y = YAML()
    return y.load(filename).dict()

def get_logger(job: JobDefinition) -> Logger:
    logger_type = job["logger"]["type"].lower()
    if logger_type == "file":
        return FileLogger()

def build_executors(job: JobDefinition) -> Dict[str, Executor]:
    executors = dict()
    for name in job["tasks"]:
        if name in executors:
            raise ExistingTaskName(f"Task with name '{name}' already exists.")
        executors[name] = Executor(name)
    for task, detl in job["tasks"].items():
        ex = executors[task]
        ex.module = detl["module"]
        ex.task = detl["task"]
        ex.logger = FileLogger(f"{job['logger']['path']}/{task}.log")
        for d in (detl.get("dependencies") or []):
            if d not in executors:
                raise ValueError(f"Dependency '{d}' does not exist.")
            executors[task].add_dependency(executors[d])
    return executors

async def initiate(args):
    filename = args[0]
    job = read_jobspec(filename)
    update_python_path(job)
    executors = build_executors(job)
    tasks = [
        asyncio.create_task(ex.start())
        for _, ex in executors.items()
    ]
    tasks.append(ProgressBar(executors).start())
    await asyncio.gather(*tasks)