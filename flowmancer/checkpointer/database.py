import pickle
import sqlite3
from contextlib import closing
from typing import Optional
from uuid import UUID, uuid4

from .checkpointer import CheckpointContents, Checkpointer, NoCheckpointAvailableError, checkpointer


@checkpointer
class SQLiteCheckpointer(Checkpointer):
    class SQLiteCheckpointerState:
        def __init__(self):
            self.con: Optional[sqlite3.Connection] = None
            self.uuid: UUID = uuid4()

    _state: SQLiteCheckpointerState = SQLiteCheckpointerState()
    checkpoint_database: str = './.flowmancer/checkpoint.db'

    @property
    def _con(self) -> sqlite3.Connection:
        if not self._state.con:
            self._state.con = sqlite3.connect(self.checkpoint_database)
            self._state.con.row_factory = sqlite3.Row
        return self._state.con

    async def on_create(self) -> None:
        self._con.execute('''
                    CREATE TABLE IF NOT EXISTS checkpoint (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uuid TEXT NOT NULL UNIQUE,
                        name TEXT NOT NULL,
                        start_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        end_ts DATETIME,
                        checkpoint_contents BLOB
                    )
                    ''')

    async def on_destroy(self) -> None:
        if self._state.con:
            self._state.con.close()

    async def write_checkpoint(self, name: str, content: CheckpointContents) -> None:
        dumped_content = sqlite3.Binary(pickle.dumps(content))
        params = [str(self._state.uuid), name, dumped_content, dumped_content]
        self._con.execute('''
                          INSERT INTO checkpoint (uuid, name, checkpoint_contents) VALUES (?, ?, ?)
                          ON CONFLICT(uuid) DO UPDATE SET checkpoint_contents = ?
                          ''', params)
        self._con.commit()

    async def read_checkpoint(self, name: str) -> CheckpointContents:
        with closing(self._con.cursor()) as cur:
            # Check if the checkpoint table exists. In the event of first ever execution for the given DB, no such
            # table will exist and therefore no checkpoints exist.
            cur.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'checkpoint'")
            row = cur.fetchone()
            if not row:
                raise NoCheckpointAvailableError(f'Checkpoint entry does not exist for: {name}')

            cur.execute('SELECT * FROM checkpoint WHERE name = ? ORDER BY id DESC LIMIT 1', [name])
            row = cur.fetchone()
            if row and not row['end_ts']:
                self._state.uuid = UUID(row['uuid'])
            else:
                raise NoCheckpointAvailableError(f'Checkpoint entry does not exist for: {name}')

            return pickle.loads(row['checkpoint_contents'])

    async def clear_checkpoint(self, _: str) -> None:
        self._con.execute(
            'UPDATE checkpoint SET end_ts = CURRENT_TIMESTAMP WHERE uuid = ?',
            [str(self._state.uuid)]
        )
        self._con.commit()
