import asyncio, sys, os, inspect
from pathlib import Path
from .executor import Executor
from .typedefs.enums import ExecutionState
from .jobspec.schema.v0_1 import JobDefinition
from .typedefs.exceptions import ExistingTaskName
from .jobspec.yaml import YAML
from .watchers.progressbar import ProgressBar
from .watchers.synchro import Synchro
from .watchers.checkpoint import Checkpoint
from .snapshot import Snapshot
from .options import parse_args

class Flowmancer:
    def __init__(self, jobdef_file: str=None):
        # To ensure all paths are resolved relative to the caller.
        self._caller_dir = Path(os.path.abspath((inspect.stack()[1])[1])).parent
        os.chdir(self._caller_dir)

        self._args = parse_args()

        self._jobspec = YAML()
        self._jobdef: JobDefinition = self._jobspec.load(self._args.jobdef or jobdef_file)
    
    def update_python_path(self):
        for p in self._jobdef.pypath:
            expanded = os.path.expandvars(os.path.expanduser(p))
            if expanded not in sys.path:
                sys.path.append(expanded)

    async def initiate(self) -> int:
        self.update_python_path()
        snapshot = Snapshot(self._jobdef.name, self._jobdef.snapshots)

        snapshot_states = dict()
        if self._args.restart:
            snapshot_states = snapshot.load_snapshot()
        
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
        tasks.append(Checkpoint(snapshot=snapshot, **watcher_kwargs).start())
        tasks.append(ProgressBar(**watcher_kwargs).start())
        
        await asyncio.gather(*tasks)
        
        failed = 0
        for ex in executors.values():
            if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                failed += 1
        if not failed:
            snapshot.delete()
        return failed
    
    def start(self) -> int:
        return asyncio.run(self.initiate())