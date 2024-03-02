import asyncio
import time
from collections import defaultdict
from typing import Dict

from rich.progress import Progress, TaskID

from ..executor import ExecutionState, ExecutionStateTransition, SerializableExecutionEvent
from .extension import Extension, extension


@extension
class RichProgressBar(Extension):
    class RichProgressBarState:
        def __init__(self) -> None:
            self.state_counts: Dict[ExecutionState, int]
            self.start_time: float
            self.progress: Progress
            self.event: asyncio.Event
            self.task: TaskID
            self.update_task: asyncio.Task

    _state: RichProgressBarState = RichProgressBarState()

    def _update_pbar(self, advance: int = 0) -> None:
        pending = self._state.state_counts[ExecutionState.PENDING]
        running = self._state.state_counts[ExecutionState.RUNNING]
        failed = (
            self._state.state_counts[ExecutionState.FAILED] + self._state.state_counts[ExecutionState.DEFAULTED]
        )
        completed = self._state.state_counts[ExecutionState.COMPLETED]
        total = (pending + running + failed + completed) or 100
        elapsed = int(time.time() - self._state.start_time)
        m = (
            f'Pending: {pending} - '
            + f'Running: {running} - '
            + f'Completed: {completed} - '
            + f'Failed: {failed} '
            + f'(Elapsed: {elapsed} sec.)'
        )
        self._state.progress.update(self._state.task, description=m, advance=advance, total=total)

    async def _continuous_update_pbar(self) -> None:
        while True:
            self._update_pbar()
            if self._state.event.is_set():
                # Need to imitate do-while to ensure pbar is updated one final time
                # before exiting loop due to Event set.
                break
            await asyncio.sleep(0.5)

    async def on_create(self) -> None:
        self._state.state_counts = defaultdict(lambda: 0)
        self._state.start_time = time.time()
        self._state.progress = Progress()
        self._state.event = asyncio.Event()
        self._state.task = self._state.progress.add_task('Pending: 0 - Running: 0 - Completed: 0 - Failed: 0')
        self._state.progress.start()

        loop = asyncio.get_event_loop()
        self._state.update_task = loop.create_task(self._continuous_update_pbar())

    async def on_destroy(self) -> None:
        self._state.event.set()
        await asyncio.gather(self._state.update_task)
        self._state.progress.stop()

    async def update(self, e: SerializableExecutionEvent) -> None:
        if isinstance(e, ExecutionStateTransition):
            from_state = ExecutionState(e.from_state)
            to_state = ExecutionState(e.to_state)
            self._state.state_counts[from_state] -= 1
            self._state.state_counts[to_state] += 1
            if to_state in (ExecutionState.FAILED, ExecutionState.COMPLETED, ExecutionState.DEFAULTED):
                self._update_pbar(1)
