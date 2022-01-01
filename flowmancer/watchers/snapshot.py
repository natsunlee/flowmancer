import time, os, pickle
from typing import Dict
from pathlib import Path
from .watcher import Watcher

def load_snapshot(snapshot_dir: str, snapshot_name: str) -> Dict[str, str]:
    snapshot_file = Path(snapshot_dir) / snapshot_name
    if not snapshot_file.exists():
        return dict()
    snapshot = pickle.load(open(snapshot_file, "rb"))
    return snapshot["states"]

class Snapshot(Watcher):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snapshot_dir = Path(kwargs["snapshot_dir"])
        self._snapshot_file = "snapshot"
        self._checkpoint = 0

    def update(self) -> None:
        if (time.time() - self._checkpoint) >= 10:
            self._checkpoint = time.time()
            self.write_snapshot()
    
    def on_destroy(self) -> None:
        # One final write to ensure final status is accurately captured.
        self.write_snapshot()

    def exists(self) -> bool:
        return (self._snapshot_dir / self._snapshot_file).exists()

    def delete(self) -> None:
        if self.exists():
            os.unlink(self._snapshot_dir / self._snapshot_file)

    def write_snapshot(self) -> None:
        snapshot = {
            "job": self.jobdef,
            "states": {
                ex.name: ex.state
                for ex in self.executors.values()
            }
        }
        tmp = self._snapshot_dir / (self._snapshot_file+".tmp")
        perm = self._snapshot_dir / self._snapshot_file
        pickle.dump(snapshot, open(tmp, 'wb'))
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)