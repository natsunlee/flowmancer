import time
from .watcher import Watcher

class Checkpoint(Watcher):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snapshot = kwargs["snapshot"]
        self._checkpoint = 0

    def update(self) -> None:
        if (time.time() - self._checkpoint) >= 10:
            self._checkpoint = time.time()
            self._snapshot.write_snapshot({
                ex.name: ex.state
                for ex in self.executors.values()
            })
    
    def on_destroy(self) -> None:
        # One final write to ensure final status is accurately captured.
        self._snapshot.write_snapshot({
                ex.name: ex.state
                for ex in self.executors.values()
            })