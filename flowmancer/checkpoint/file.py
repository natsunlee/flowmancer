import os
import pickle
from pathlib import Path

from . import (Checkpoint, CheckpointContents, NoCheckpointAvailableError,
               checkpoint)


@checkpoint
class FileCheckpoint(Checkpoint):
    def __init__(self, checkpoint_name: str, checkpoint_dir: str = './.flowmancer') -> None:
        self.checkpoint_name = checkpoint_name
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_file_path = self.checkpoint_dir / self.checkpoint_name

    def write_checkpoint(self, content: CheckpointContents) -> None:
        if not os.path.exists(self.checkpoint_dir):
            os.makedirs(self.checkpoint_dir, exist_ok=True)
        tmp = self.checkpoint_dir / (self.checkpoint_name + '.tmp')
        perm = self.checkpoint_file_path
        with open(tmp, 'wb') as f:
            pickle.dump(content, f)
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)

    def read_checkpoint(self) -> CheckpointContents:
        checkpoint_file = self.checkpoint_file_path
        if not checkpoint_file.exists():
            raise NoCheckpointAvailableError(f'Checkpoint file does not exist: {self.checkpoint_file_path}')
        return pickle.load(open(checkpoint_file, 'rb'))

    def clear_checkpoint(self) -> None:
        cfile = self.checkpoint_file_path
        if os.path.isfile(cfile):
            os.unlink(cfile)
