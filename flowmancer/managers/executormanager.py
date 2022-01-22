import asyncio
from collections import defaultdict
from typing import Set, Any, List
from ..executors.executor import Executor
from ..executors.local import LocalExecutor
from ..typedefs.models import JobDefinition
from ..typedefs.exceptions import ExistingTaskName
from ..typedefs.enums import ExecutionState

class ExecutorManager:

    def __init__(self, jobdef: JobDefinition) -> None:
        self.executors = dict()
        self._jobdef = jobdef
        self._children = defaultdict(lambda:set())
        self._states = defaultdict(lambda:set())

        for name, detl in self._jobdef.tasks.items():
            if name in self.executors:
                raise ExistingTaskName(f"Task with name '{name}' already exists.")
            ex = LocalExecutor(name, detl, self._jobdef.loggers, lambda x: self.executors[x], self._notify_state_transition)
            self.executors[name] = ex
            self._states[ex.state].add(ex.name)
            for n in ex.dependencies:
                self._children[n].add(ex.name)

    def set_state_for_executor(self, name: str, to_state: ExecutionState) -> None:
        ex = self.executors[name]
        ex.state = to_state

    def num_executors_in_state(self, *args: List[ExecutionState]) -> int:
        ret = 0
        for s in args:
            ret += len(self._states[s])
        return ret

    def get_executors_in_state(self, *args: List[ExecutionState]) -> List[str]:
        ret = []
        for s in args:
            ret.extend([ n for n in self._states[s] ])
        return ret

    def get_children(self, name: str) -> Set[str]:
        return self._children[name]

    # Only to be used by Executors to communicate state changes back to this manager
    def _notify_state_transition(self, name: str, from_state: ExecutionState, to_state: ExecutionState) -> None:
        self._states[from_state].remove(name)
        self._states[to_state].add(name)

    def create_tasks(self) -> List[asyncio.Task]:
        return [
            asyncio.create_task(ex.start())
            for ex in self.executors.values()
        ]

    # Map dictionary methods to Executors dictionary
    def __contains__(self, key: str):
        return key in self.executors
    def __iter__(self):
        return iter(self.executors)
    def __setitem__(self, key: str, item: Any):
        self.executors[key] = item
    def __getitem__(self, key: str) -> Executor:
        return self.executors[key]
    def __len__(self) -> int:
        return len(self.executors)
    def __delitem__(self, key: str) -> None:
        del self.executors[key]
    def keys(self):
        return self.executors.keys()
    def values(self):
        return self.executors.values()
    def items(self):
        return self.executors.items()