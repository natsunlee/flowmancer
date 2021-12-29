import time, asyncio
from .watcher import Watcher
from ..typedefs.enums import ExecutionState
from collections import defaultdict

class Monitor(Watcher):
    async def start(self) -> None:
        executors_by_state = defaultdict(lambda:set())
        for _, ex in self.executors.items():
            executors_by_state[ex.state].add(ex)

        is_complete = False
        start_time = time.time()
        while not is_complete:
            is_complete = True
            for state in (ExecutionState.PENDING, ExecutionState.RUNNING):
                for ex in executors_by_state[state].copy():
                    if ex.is_alive:
                        is_complete = False
                    if ex.state != state:
                        executors_by_state[state].remove(ex)
                        executors_by_state[ex.state].add(ex)
            print("Pending: {} | Running: {} | Completed: {} | Failed: {} | Defaulted: {} | Time Elapsed: {:.1f} sec.".format(
                len(executors_by_state[ExecutionState.PENDING]),
                len(executors_by_state[ExecutionState.RUNNING]),
                len(executors_by_state[ExecutionState.COMPLETED]),
                len(executors_by_state[ExecutionState.FAILED]),
                len(executors_by_state[ExecutionState.DEFAULTED]),
                time.time() - start_time
            ))
            if not is_complete:
                await asyncio.sleep(1)
        self._event.set()