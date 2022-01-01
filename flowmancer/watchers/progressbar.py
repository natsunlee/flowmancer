from .watcher import Watcher
from tqdm import tqdm
from ..typedefs.enums import ExecutionState

class ProgressBar(Watcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
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
                if ex.state == ExecutionState.RUNNING: running += 1
        self.pbar.set_description(f"Running: {running} - Failed: {self.failed}")
    
    def on_destroy(self) -> None:
        self.pbar.close()