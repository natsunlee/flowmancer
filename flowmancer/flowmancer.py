import asyncio, sys, os, inspect
from pathlib import Path
from .managers.executormanager import ExecutorManager
from .managers.observermanager import ObserverManager
from .executors.executor import Executor
from .typedefs.enums import ExecutionState
from .typedefs.models import JobDefinition
from .typedefs.exceptions import MissingJobDef, ExecutorDoesNotExist
from .jobspec.yaml import YAML
from .observers.observer import Observer
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

        # Update Python path
        for p in self._jobdef.pypath:
            expanded = os.path.expandvars(os.path.expanduser(p))
            if expanded not in sys.path:
                sys.path.append(expanded)

        self._executor_manager = ExecutorManager(self._jobdef)
        
        # Initialize global Observer properties
        Observer.executors = self._executor_manager

        if self._args.restart:
            Observer.restart = True
            Executor.restart = True

        # Process skips
        for name in self._args.skip:
            if name not in self._executor_manager:
                raise ExecutorDoesNotExist(f"Executor with name '{name}' does not exist.")
            self._executor_manager.set_state_for_executor(name, ExecutionState.SKIP)

        # Process run-to
        if self._args.run_to:
            if self._args.run_to not in self._executor_manager:
                raise ExecutorDoesNotExist(f"Executor with name '{self._args.run_to}' does not exist.")
            stack = [self._executor_manager[self._args.run_to]]
            enabled = set()
            while stack:
                cur = stack.pop()
                enabled.add(cur.name)
                stack.extend([ self._executor_manager[n] for n in cur.dependencies ])
            for name, ex in self._executor_manager.items():
                if name not in enabled: ex.state = ExecutionState.SKIP
        
        # Process run-from
        if self._args.run_from:
            if self._args.run_from not in self._executor_manager:
                raise ExecutorDoesNotExist(f"Executor with name '{self._args.run_from}' does not exist.")
            stack = [self._executor_manager[self._args.run_from]]
            enabled = set()
            while stack:
                cur = stack.pop()
                enabled.add(cur.name)
                stack.extend([ self._executor_manager[n] for n in self._executor_manager.get_children(cur.name) ])
            for name, ex in self._executor_manager.items():
                if name not in enabled: ex.state = ExecutionState.SKIP

    async def initiate(self) -> int:
        # Set max parallel limit
        if self._jobdef.concurrency:
            Executor.semaphore = asyncio.Semaphore(self._jobdef.concurrency)
        observer_manager = ObserverManager(self._jobdef.observers)
        tasks = observer_manager.create_tasks() + self._executor_manager.create_tasks()
        
        await asyncio.gather(*tasks)
        
        return self._executor_manager.num_executors_in_state(
            ExecutionState.FAILED,
            ExecutionState.DEFAULTED
        )
    
    def start(self) -> int:
        return asyncio.run(self.initiate())