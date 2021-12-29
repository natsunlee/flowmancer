import asyncio, sys
from typing import Dict
from .executor import Executor
from .typedefs.enums import ExecutionState
from .jobspec.schema.v0_1 import JobDefinition
from .typedefs.exceptions import ExistingTaskName
from .jobspec.yaml import YAML
from .watchers.progressbar import ProgressBar
from .watchers.monitor import Monitor
from .logger.file import FileLogger
from .logger.logger import Logger

class Flowmancer:
    def __init__(self, jobspec_file: str):
        self._jobspec = YAML()
        self._job: JobDefinition = self._jobspec.load(jobspec_file).dict()

    def get_logger(self):
        pass
    
    def update_python_path(self):
        for p in (self._job.get("pypath") or []):
            if p not in sys.path:
                sys.path.append(p)

    def build_executors(self) -> Dict[str, Executor]:
        executors = dict()
        for name in self._job["tasks"]:
            if name in executors:
                raise ExistingTaskName(f"Task with name '{name}' already exists.")
            executors[name] = Executor(name)
        for task, detl in self._job["tasks"].items():
            ex = executors[task]
            ex.module = detl["module"]
            ex.task = detl["task"]
            ex.logger = FileLogger(f"{self._job['logger']['path']}/{task}.log")
            for d in (detl.get("dependencies") or []):
                if d not in executors:
                    raise ValueError(f"Dependency '{d}' does not exist.")
                executors[task].add_dependency(executors[d])
        return executors

    async def initiate(self) -> int:
        self.update_python_path()
        executors = self.build_executors()
        tasks = [
            asyncio.create_task(ex.start())
            for ex in executors.values()
        ]
        tasks.append(ProgressBar(executors).start())
        #tasks.append(Monitor(executors).start())
        await asyncio.gather(*tasks)
        
        failed = 0
        for ex in executors.values():
            if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                failed += 1
        return failed
    
    def start(self) -> int:
        return asyncio.run(self.initiate())