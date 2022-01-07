import os, pickle
from typing import Dict
from pathlib import Path
from .typedefs.models import SnapshotsDefinition
from .typedefs.enums import ExecutionState

class Snapshot:
    def __init__(self, snapshot_name: str, snapdef: SnapshotsDefinition) -> None:
        self._snapshot_name = snapshot_name
        self._snapshot_dir = Path(snapdef.path)
        self._snapshot_dir.mkdir(parents=True, exist_ok=True)

    def exists(self) -> bool:
        return (self._snapshot_dir / self._snapshot_name).exists()

    def delete(self) -> None:
        if self.exists():
            os.unlink(self._snapshot_dir / self._snapshot_name)

    def write_snapshot(self, states: Dict[str, ExecutionState]) -> None:
        snapshot = { "states": states }
        tmp = self._snapshot_dir / (self._snapshot_name+".tmp")
        perm = self._snapshot_dir / self._snapshot_name
        pickle.dump(snapshot, open(tmp, 'wb'))
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)
    
    def load_snapshot(self) -> Dict[str, ExecutionState]:
        snapshot_file = Path(self._snapshot_dir) / self._snapshot_name
        if not snapshot_file.exists():
            return dict()
        snapshot = pickle.load(open(snapshot_file, "rb"))
        return snapshot["states"]