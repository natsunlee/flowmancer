import os
import pickle
from pathlib import Path

from .checkpointer import CheckpointContents, Checkpointer, NoCheckpointAvailableError, checkpointer


@checkpointer
class FileCheckpointer(Checkpointer):
    checkpoint_dir: str = './.flowmancer'

    async def write_checkpoint(self, name: str, content: CheckpointContents) -> None:
        cdir = Path(self.checkpoint_dir)
        if not os.path.exists(cdir):
            os.makedirs(cdir, exist_ok=True)
        tmp = cdir / (name + '.tmp')
        perm = Path(self.checkpoint_dir) / name
        with open(tmp, 'wb') as f:
            pickle.dump(content, f)
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)

    async def read_checkpoint(self, name: str) -> CheckpointContents:
        checkpoint_file = Path(self.checkpoint_dir) / name
        if not checkpoint_file.exists():
            raise NoCheckpointAvailableError(f'Checkpoint file does not exist: {checkpoint_file}')
        return pickle.load(open(checkpoint_file, 'rb'))

    async def clear_checkpoint(self, name: str) -> None:
        cfile = Path(self.checkpoint_dir) / name
        if os.path.isfile(cfile):
            os.unlink(cfile)
