import time, os, pickle
from typing import Dict
from pathlib import Path
from .observer import Observer
from ..typedefs.enums import ExecutionState

class Checkpoint(Observer):
    def __init__(self, **kwargs) -> None:
        self._checkpoint_name = kwargs.pop("checkpoint_name")
        self._checkpoint_dir = Path(kwargs.pop("checkpoint_dir"))
        self._checkpoint_time = 0
        super().__init__(**kwargs)

        # Ensure checkpoint directory exists
        if not self._checkpoint_dir.exists():
            os.makedirs(self._checkpoint_dir, exist_ok = True)

    def on_restart(self) -> None:
        checkpoint = self._load_checkpoint()
        for name, state in checkpoint["states"].items():
            if state in (ExecutionState.COMPLETED, ExecutionState.SKIP):
                self.executors.set_state_for_executor(name, state)
            if state == ExecutionState.FAILED:
                self.executors.set_restart_flag_for_executor(name)
        for k,v in checkpoint["stash"].items():
            self.executors.stash[k] = v

    def update(self) -> None:
        if (time.time() - self._checkpoint_time) >= 10:
            self._checkpoint_time = time.time()
            self._write_checkpoint()

    def on_success(self) -> None:
        # Checkpoint not needed for successful jobs.
        self._delete_checkpoint()

    def on_failure(self) -> None:
        # One final write to ensure final status is accurately captured.
        self._write_checkpoint()

    def on_abort(self) -> None:
        # One final write to ensure final status is accurately captured.
        self._write_checkpoint()

    def _delete_checkpoint(self) -> None:
        if (self._checkpoint_dir / self._checkpoint_name).exists():
            os.unlink(self._checkpoint_dir / self._checkpoint_name)

    def _write_checkpoint(self) -> None:
        states = {
            ex.name: ex.state
            for ex in self.executors.values()
        }
        checkpoint = { "states": states, "stash": self.executors.stash.copy() }
        tmp = self._checkpoint_dir / (self._checkpoint_name+".tmp")
        perm = self._checkpoint_dir / self._checkpoint_name
        pickle.dump(checkpoint, open(tmp, 'wb'))
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)
    
    def _load_checkpoint(self) -> Dict[str, ExecutionState]:
        checkpoint_file = Path(self._checkpoint_dir) / self._checkpoint_name
        if not checkpoint_file.exists():
            return dict()
        checkpoint = pickle.load(open(checkpoint_file, "rb"))
        return checkpoint