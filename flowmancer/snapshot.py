import pickle, os
from typing import Dict, Tuple
from pathlib import Path
from .executor import Executor
from .jobspec.schema.v0_1 import JobDefinition

class Snapshot:
    def __init__(self, job: JobDefinition, executors: Dict[str, Executor], snapshot_dir: Path) -> None:
        self._job = job
        self._executors = executors
        self._snapshot_dir = snapshot_dir
    
    def write_snapshot(self) -> None:
        snapshot = {
            "job": self._job,
            "states": {
                ex.name: ex.state
                for ex in self._executors.values()
            }
        }
        tmp = f"{str(self._snapshot_dir)}/something.tmp"
        perm = f"{str(self._snapshot_dir)}/something"
        pickle.dump(snapshot, open(tmp, 'wb'))
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)
    
    def load_snapshot(self) -> Tuple(JobDefinition, Dict[str, str]):
        snapshot_file = f"{str(self._snapshot_dir)}/something"
        snapshot = pickle.load(open(snapshot_file, "rb"))
        return snapshot["job"], snapshot["states"]