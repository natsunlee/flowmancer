import glob
import os
import time
from datetime import datetime
from typing import Dict, TextIO

from ..eventbus.log import LogEndEvent, LogStartEvent, LogWriteEvent, SerializableLogEvent
from .logger import Logger, logger


class LogFileIsAlreadyOpen(Exception):
    pass


class LogFileNotOpen(Exception):
    pass


@logger
class FileLogger(Logger):
    class FileLoggerState:
        def __init__(self) -> None:
            self.file_handles: Dict[str, TextIO] = dict()
            self.ts_str = datetime.now().strftime('%Y-%m-%d.%H.%M.%S')

    _state: FileLoggerState = FileLoggerState()
    base_log_dir: str = './logs'
    retention_days: int = 10

    @property
    def log_dir(self) -> str:
        return f'{self.base_log_dir}/{self._state.ts_str}'

    async def on_create(self) -> None:
        os.makedirs(self.log_dir, exist_ok=True)

    async def update(self, msg: SerializableLogEvent) -> None:
        if isinstance(msg, LogStartEvent):
            f = self._state.file_handles.get(msg.name)
            if f and not f.closed:
                raise LogFileIsAlreadyOpen(f'Log file is already open for {msg.name}')
            self._state.file_handles[msg.name] = open(f'{self.log_dir}/{msg.name}.log', 'a')
        elif isinstance(msg, LogEndEvent):
            f = self._state.file_handles.get(msg.name)
            if f and not f.closed:
                f.close()
        elif isinstance(msg, LogWriteEvent):
            f = self._state.file_handles.get(msg.name)
            if not f or f.closed:
                raise LogFileNotOpen(f'Log file is not open for {msg.name}')
            template = '[{ts}] {sev} - {m}\n'
            f.write(template.format(
                sev=msg.severity.value,
                ts=msg.timestamp,
                m=msg.message
            ))

    async def on_destroy(self) -> None:
        for f in self._state.file_handles.values():
            if not f.closed:
                f.close()

        if self.retention_days < 0:
            return

        def _should_delete_file(fpath):
            return os.stat(fpath).st_mtime < (time.time() - (self.retention_days * 86400.0))

        def _should_delete_dir(dpath):
            return os.path.isdir(dpath) and not os.listdir(dpath)

        files = filter(_should_delete_file, glob.glob(f'{self.base_log_dir}/**/*.log'))
        # Type ignore hints due to mypy bugging out and detecting incorrect type...
        for f in files:  # type: ignore
            print(f'Deleting Log File: {f}')
            os.remove(f)  # type: ignore

        dirs = filter(_should_delete_dir, [f'{self.base_log_dir}/{p}' for p in os.listdir(self.base_log_dir)])
        for d in dirs:
            print(f'Deleting Directory: {d}')
            os.rmdir(d)
