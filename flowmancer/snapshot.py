import pickle, os
from typing import Dict, Tuple
from pathlib import Path
from .executor import Executor
from .jobspec.schema.v0_1 import JobDefinition

def write_snapshot(snapshot_path: Path, snapshot_name: str) -> None:
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

def load_snapshot() -> Tuple[JobDefinition, Dict[str, str]]:
    snapshot_file = self._snapshot_dir / self._snapshot_file
    snapshot = pickle.load(open(snapshot_file, "rb"))
    return snapshot["job"], snapshot["states"]