import os
import pickle
from pathlib import Path

from .checkpoint import Checkpoint, CheckpointContents, NoCheckpointAvailableError, checkpoint


@checkpoint
class FileCheckpoint(Checkpoint):
    checkpoint_name: str
    checkpoint_dir: str = './.flowmancer'

    @property
    def checkpoint_file_path(self) -> Path:
        return Path(self.checkpoint_dir) / self.checkpoint_name

    def write_checkpoint(self, content: CheckpointContents) -> None:
        cdir = Path(self.checkpoint_dir)
        if not os.path.exists(cdir):
            os.makedirs(cdir, exist_ok=True)
        tmp = cdir / (self.checkpoint_name + '.tmp')
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
