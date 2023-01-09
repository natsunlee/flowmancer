import time

from rich.progress import Progress
from tqdm import tqdm

from ..typedefs.enums import ExecutionState
from .observer import Observer


class TqdmProgressBar(Observer):
    def on_create(self) -> None:
        self.pending = set(self.executors.values())
        self.total = len(self.pending)
        self.failed = 0
        self.pbar = tqdm(total=self.total)

    def update(self) -> None:
        running = 0
        for ex in self.pending.copy():
            if not ex.is_alive:
                self.pbar.update(1)
                self.pending.remove(ex)
                if ex.state in (ExecutionState.FAILED, ExecutionState.DEFAULTED):
                    self.failed += 1
            else:
                if ex.state == ExecutionState.RUNNING:
                    running += 1
        self.pbar.set_description(f"Running: {running} - Failed: {self.failed}")

    def on_destroy(self) -> None:
        self.pbar.close()


class RichProgressBar(Observer):
    def on_create(self) -> None:
        self.pending = set(self.executors.values())
        self.total = len(self.pending)
        self.failed = 0
        self.start_time = time.time()
        self.progress = Progress()
        self.task = self.progress.add_task("Running: 0 - Failed: 0", total=self.total)
        self.progress.start()

    def on_destroy(self) -> None:
        self.progress.stop()

    def update(self) -> None:
        running = 0
        elapsed = int(time.time() - self.start_time)
        for ex in self.pending.copy():
            if not ex.is_alive:
                self.progress.advance(self.task)
                self.pending.remove(ex)
                if ex.state == ExecutionState.FAILED:
                    self.failed += 1
                    self.progress.console.print(f"Task Failed: {ex.name}")
                elif ex.state == ExecutionState.COMPLETED:
                    self.progress.console.print(f"Task Completed: {ex.name}")
            else:
                if ex.state == ExecutionState.RUNNING:
                    running += 1
        self.progress.update(
            self.task, description=f"Running: {running} - Failed: {self.failed} (Elapsed: {elapsed} sec.)"
        )
