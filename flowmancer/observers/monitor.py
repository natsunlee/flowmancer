import time
from .observer import Observer
from ..typedefs.enums import ExecutionState
from collections import defaultdict

class Monitor(Observer):
    
    def on_create(self) -> None:
        self.executors_by_state = defaultdict(lambda:set())
        for ex in self.executors.values():
            self.executors_by_state[ex.state].add(ex)
        self.start_time = time.time()
    
    def update(self) -> None:
        for state in (ExecutionState.PENDING, ExecutionState.RUNNING):
            for ex in self.executors_by_state[state].copy():
                if ex.state != state:
                    self.executors_by_state[state].remove(ex)
                    self.executors_by_state[ex.state].add(ex)
        print("Pending: {} | Running: {} | Completed: {} | Failed: {} | Defaulted: {} | Time Elapsed: {:.1f} sec.".format(
            len(self.executors_by_state[ExecutionState.PENDING]),
            len(self.executors_by_state[ExecutionState.RUNNING]),
            len(self.executors_by_state[ExecutionState.COMPLETED]),
            len(self.executors_by_state[ExecutionState.FAILED]),
            len(self.executors_by_state[ExecutionState.DEFAULTED]),
            time.time() - self.start_time
        ))