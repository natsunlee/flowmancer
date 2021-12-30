import asyncio, sys, os, inspect
from typing import Dict
from pathlib import Path
from .executor import Executor
from .typedefs.enums import ExecutionState
from .jobspec.schema.v0_1 import JobDefinition
from .typedefs.exceptions import ExistingTaskName
from .jobspec.yaml import YAML
from .watchers.progressbar import ProgressBar
from .watchers.monitor import Monitor
from .watchers.synchro import Synchro
from .watchers.snapshot import Snapshot, load_snapshot
from .logger.file import FileLogger
from .logger.logger import Logger

class Flowmancer:
    def __init__(self, jobdef_file: str):
        self._jobspec = YAML()
        self._jobdef: JobDefinition = self._jobspec.load(jobdef_file)
        self._caller_dir = Path(os.path.abspath((inspect.stack()[1])[1])).parent

    def get_logger(self):
        pass
    
    def update_python_path(self):
        for p in (self._jobdef.pypath or []):
            expanded = os.path.expandvars(os.path.expanduser(p))
            if expanded not in sys.path:
                sys.path.append(expanded)

    def build_executors(self, states: Dict[str, str] = None) -> Dict[str, Executor]:
        executors = dict()
        for name in self._jobdef.tasks:
            if name in executors:
                raise ExistingTaskName(f"Task with name '{name}' already exists.")
            ex = Executor(name)
            if states:
                saved_state = states[name]
                if saved_state not in (ExecutionState.COMPLETED, ExecutionState.NORUN):
                    saved_state = ExecutionState.PENDING
                ex.state = saved_state
            executors[name] = ex
        for task, detl in self._jobdef.tasks.items():
            ex = executors[task]
            ex.module = detl.module
            ex.task = detl.task
            expanded = os.path.expandvars(os.path.expanduser(self._jobdef.loggers.file.path))
            ex.logger = FileLogger(f"{expanded}/{task}.log")
            for d in (detl.dependencies or []):
                if d not in executors:
                    raise ValueError(f"Dependency '{d}' does not exist.")
                executors[task].add_dependency(executors[d])
        return executors

    async def initiate(self) -> int:
        self.update_python_path()
        executors = self.build_executors(load_snapshot("./temp", "snapshot"))
        tasks = [
            asyncio.create_task(ex.start())
            for ex in executors.values()
        ]

        watcher_kwargs = {
            "executors": executors,
            "jobdef": self._jobdef
        }
        tasks.append(ProgressBar(**watcher_kwargs).start_wrapper())
        tasks.append(Synchro(**watcher_kwargs).start_wrapper())
        tasks.append(Snapshot(snapshot_dir="./temp", **watcher_kwargs).start_wrapper())
        #tasks.append(Monitor(executors).start_wrapper())
        
        await asyncio.gather(*tasks)
        
        failed = 0
        for ex in executors.values():
            if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                failed += 1
        return failed
    
    def start(self) -> int:
        return asyncio.run(self.initiate())