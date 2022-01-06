import time
from .observer import Observer

class Checkpoint(Observer):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snapshot = kwargs["snapshot"]
        self._checkpoint = 0
    
    def _write_snapshot(self) -> None:
        self._snapshot.write_snapshot({
                ex.name: ex.state
                for ex in self.executors.values()
            })

    def update(self) -> None:
        if (time.time() - self._checkpoint) >= 10:
            self._checkpoint = time.time()
            self._write_snapshot()
    
    def on_destroy(self) -> None:
        if self.failed_executors_exist():
            # One final write to ensure final status is accurately captured.
            self._write_snapshot()
        else:
            # Checkpoint not needed for successful jobs.
            self._snapshot.delete()