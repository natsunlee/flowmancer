import asyncio, sys, os, inspect, signal
from typing import List
from pathlib import Path
from .managers.executormanager import ExecutorManager
from .managers.observermanager import ObserverManager
from .typedefs.enums import ExecutionState
from .typedefs.models import JobDefinition
from .typedefs.exceptions import MissingJobDef, ExecutorDoesNotExist
from .jobspecs.yaml import YAML
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

    async def initiate(self) -> int:
        # Update Python path
        for p in self._jobdef.pypath:
            expanded = os.path.expandvars(os.path.expanduser(p))
            if expanded not in sys.path:
                sys.path.append(expanded)

        executor_manager = ExecutorManager(self._jobdef)
        observer_manager = ObserverManager(self._jobdef.observers, executor_manager, self._args.restart)

        # Process skips
        for name in self._args.skip:
            if name not in executor_manager:
                raise ExecutorDoesNotExist(f"Executor with name '{name}' does not exist.")
            executor_manager.set_state_for_executor(name, ExecutionState.SKIP)

        # Process run-to
        if self._args.run_to:
            if self._args.run_to not in executor_manager:
                raise ExecutorDoesNotExist(f"Executor with name '{self._args.run_to}' does not exist.")
            stack = [executor_manager[self._args.run_to]]
            enabled = set()
            while stack:
                cur = stack.pop()
                enabled.add(cur.name)
                stack.extend([ executor_manager[n] for n in cur.dependencies ])
            for name, ex in executor_manager.items():
                if name not in enabled: ex.state = ExecutionState.SKIP
        
        # Process run-from
        if self._args.run_from:
            if self._args.run_from not in executor_manager:
                raise ExecutorDoesNotExist(f"Executor with name '{self._args.run_from}' does not exist.")
            stack = [executor_manager[self._args.run_from]]
            enabled = set()
            while stack:
                cur = stack.pop()
                enabled.add(cur.name)
                stack.extend([ executor_manager[n] for n in executor_manager.get_children(cur.name) ])
            for name, ex in executor_manager.items():
                if name not in enabled: ex.state = ExecutionState.SKIP

        tasks = observer_manager.create_tasks() + executor_manager.create_tasks()

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda tasks=tasks: asyncio.create_task(self.terminate(tasks)))
        loop.add_signal_handler(signal.SIGINT, lambda tasks=tasks: asyncio.create_task(self.terminate(tasks)))
        
        await asyncio.gather(*tasks)
        
        return executor_manager.num_executors_in_state(
            ExecutionState.FAILED,
            ExecutionState.DEFAULTED
        )
    
    async def terminate(self, tasks: List[asyncio.Task]):
        for t in tasks:
            t.cancel()

    def start(self) -> int:
        return asyncio.run(self.initiate())