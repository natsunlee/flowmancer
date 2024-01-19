import asyncio
import time
from collections import defaultdict
from typing import Dict

from rich.progress import Progress

from ..executor import ExecutionState, ExecutionStateTransition, SerializableExecutionEvent
from . import Observer, observer


@observer
class RichProgressBar(Observer):
    def _update_pbar(self, advance: int = 0) -> None:
        pending = self.state_counts[ExecutionState.PENDING]
        running = self.state_counts[ExecutionState.RUNNING]
        failed = self.state_counts[ExecutionState.FAILED] + self.state_counts[ExecutionState.DEFAULTED]
        completed = self.state_counts[ExecutionState.COMPLETED]
        total = (pending + running + failed + completed) or 100
        elapsed = int(time.time() - self.start_time)
        m = (
            f'Pending: {pending} - '
            + f'Running: {running} - '
            + f'Completed: {completed} - '
            + f'Failed: {failed} '
            + f'(Elapsed: {elapsed} sec.)'
        )
        self.progress.update(self.task, description=m, advance=advance, total=total)

    async def _continuous_update_pbar(self) -> None:
        while True:
            self._update_pbar()
            if self._event.is_set():
                # Need to imitate do-while to ensure pbar is updated one final time
                # before exiting loop due to Event set.
                break
            await asyncio.sleep(0.5)

    async def on_create(self) -> None:
        self.state_counts: Dict[ExecutionState, int] = defaultdict(lambda: 0)
        self.start_time = time.time()
        self.progress = Progress()
        self._event = asyncio.Event()
        self.task = self.progress.add_task('Pending: 0 - Running: 0 - Completed: 0 - Failed: 0')
        self.progress.start()

        loop = asyncio.get_event_loop()
        self._update_task = loop.create_task(self._continuous_update_pbar())

    async def on_destroy(self) -> None:
        self._event.set()
        await asyncio.gather(self._update_task)
        self.progress.stop()

    async def update(self, e: SerializableExecutionEvent) -> None:
        if isinstance(e, ExecutionStateTransition):
            from_state = ExecutionState(e.from_state)  # type: ignore
            to_state = ExecutionState(e.to_state)  # type: ignore
            self.state_counts[from_state] -= 1
            self.state_counts[to_state] += 1
            if to_state in (ExecutionState.FAILED, ExecutionState.COMPLETED, ExecutionState.DEFAULTED):
                self._update_pbar(1)
