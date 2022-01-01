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
from .options import parse_args

class Flowmancer:
    def __init__(self, jobdef_file: str):
        # To ensure all paths are resolved relative to the caller.
        self._caller_dir = Path(os.path.abspath((inspect.stack()[1])[1])).parent
        os.chdir(self._caller_dir)

        self._jobspec = YAML()
        self._jobdef: JobDefinition = self._jobspec.load(jobdef_file)
    
    def update_python_path(self):
        for p in self._jobdef.pypath:
            expanded = os.path.expandvars(os.path.expanduser(p))
            if expanded not in sys.path:
                sys.path.append(expanded)

    async def initiate(self) -> int:
        self.update_python_path()

        snapshot_states = dict()
        args = parse_args()
        if args.restart:
            snapshot_states = load_snapshot("./temp", "snapshot")
        
        executors = dict()
        for name, taskdef in self._jobdef.tasks.items():
            if name in executors:
                raise ExistingTaskName(f"Task with name '{name}' already exists.")
            executors[name] = Executor(name, taskdef, self._jobdef.loggers, lambda x: executors[x], snapshot_states.get(name))
        tasks = [
            asyncio.create_task(ex.start())
            for ex in executors.values()
        ]

        watcher_kwargs = {
            "executors": executors,
            "jobdef": self._jobdef
        }
        tasks.append(Synchro(**watcher_kwargs).start())
        tasks.append(Snapshot(snapshot_dir="./temp", **watcher_kwargs).start())
        #tasks.append(ProgressBar(**watcher_kwargs).start())
        tasks.append(Monitor(**watcher_kwargs).start())
        
        await asyncio.gather(*tasks)
        
        failed = 0
        for ex in executors.values():
            if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                failed += 1
        return failed
    
    def start(self) -> int:
        return asyncio.run(self.initiate())