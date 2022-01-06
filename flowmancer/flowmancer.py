import asyncio, sys, os, inspect
from pathlib import Path
from .executor import Executor
from .typedefs.enums import ExecutionState
from .jobspec.schema.v0_1 import JobDefinition
from .typedefs.exceptions import ExistingTaskName, MissingJobDef, ExecutorDoesNotExist
from .jobspec.yaml import YAML
from .observers.progressbar import ProgressBar
from .observers.synchro import Synchro
from .observers.checkpoint import Checkpoint
from .snapshot import Snapshot
from .options import parse_args

class Flowmancer:
    def __init__(self, jobdef_file: str=None):
        # Update CWD ensure all paths are resolved relative to the caller.
        self._caller_dir = Path(os.path.abspath((inspect.stack()[1])[1])).parent
        os.chdir(self._caller_dir)

        self._args = parse_args()
        jfile = self._args.jobdef or jobdef_file
        if not jfile:
            raise MissingJobDef("No job definition file has been provided.")

        # Read job definition
        self._jobspec = YAML()
        self._jobdef: JobDefinition = self._jobspec.load(jfile)
        self._snapshot = Snapshot(self._jobdef.name, self._jobdef.snapshots)

        # Initialize executors
        self._executors = dict()
        for name, taskdef in self._jobdef.tasks.items():
            if name in self._executors:
                raise ExistingTaskName(f"Task with name '{name}' already exists.")
            self._executors[name] = Executor(name, taskdef, self._jobdef.loggers, lambda x: self._executors[x])
        
        # Restore prior states if restart
        if self._args.restart:
            for name, state in self._snapshot.load_snapshot().items():
                if state in (ExecutionState.COMPLETED, ExecutionState.SKIP):
                    self._executors[name].state = state

        # Process skips
        for name in self._args.skip:
            if name not in self._executors:
                raise ExecutorDoesNotExist(f"Executor with name '{name}' does not exist.")
            self._executors[name].state = ExecutionState.SKIP

        # Process run-to
        if self._args.run_to:
            if self._args.run_to not in self._executors:
                raise ExecutorDoesNotExist(f"Executor with name '{name}' does not exist.")
            stack = [self._executors[self._args.run_to]]
            enabled = set()
            while stack:
                cur = stack.pop()
                print(f"task: {cur.name} | dep: {cur.dependencies}")
                enabled.add(cur.name)
                stack.extend([ self._executors[n] for n in cur.dependencies ])
            for name, ex in self._executors.items():
                if name not in enabled: ex.state = ExecutionState.SKIP

    def update_python_path(self):
        for p in self._jobdef.pypath:
            expanded = os.path.expandvars(os.path.expanduser(p))
            if expanded not in sys.path:
                sys.path.append(expanded)

    async def initiate(self) -> int:
        self.update_python_path()

        tasks = [
            asyncio.create_task(ex.start())
            for ex in self._executors.values()
        ]

        observer_kwargs = {
            "executors": self._executors,
            "jobdef": self._jobdef
        }
        tasks.append(Synchro(**observer_kwargs).start())
        tasks.append(Checkpoint(snapshot=self._snapshot, **observer_kwargs).start())
        tasks.append(ProgressBar(**observer_kwargs).start())
        
        await asyncio.gather(*tasks)
        
        failed = 0
        for ex in self._executors.values():
            if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                failed += 1
        return failed
    
    def start(self) -> int:
        return asyncio.run(self.initiate())