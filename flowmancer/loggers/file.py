import glob
import os
import time
from datetime import datetime
from typing import Dict, TextIO

from ..eventbus.log import (LogEndEvent, LogStartEvent, LogWriteEvent,
                            SerializableLogEvent)
from . import Logger, logger


class LogFileIsAlreadyOpen(Exception):
    pass


class LogFileNotOpen(Exception):
    pass


@logger
class FileLogger(Logger):
    def __init__(self, **kwargs: str) -> None:
        self._base_log_dir = kwargs.get('log_dir', './logs')
        ts_str = datetime.now().strftime('%Y-%m-%d.%H.%M.%S')
        self._log_dir = f'{self._base_log_dir}/{ts_str}'
        os.makedirs(self._log_dir, exist_ok=True)
        self._file_handles: Dict[str, TextIO] = dict()
        self._retention_days = 10

    async def update(self, msg: SerializableLogEvent) -> None:
        if isinstance(msg, LogStartEvent):
            f = self._file_handles.get(msg.name)
            if f and not f.closed:
                raise LogFileIsAlreadyOpen(f'Log file is already open for {msg.name}')
            self._file_handles[msg.name] = open(f'{self._log_dir}/{msg.name}.log', 'a')
        elif isinstance(msg, LogEndEvent):
            f = self._file_handles.get(msg.name)
            if f and not f.closed:
                f.close()
        elif isinstance(msg, LogWriteEvent):
            f = self._file_handles.get(msg.name)
            if not f or f.closed:
                raise LogFileNotOpen(f'Log file is not open for {msg.name}')
            f.write(msg.message)

    async def on_destroy(self) -> None:
        for f in self._file_handles.values():
            if not f.closed:
                f.close()

        if self._retention_days < 0:
            return

        def _should_delete_file(fpath):
            return os.stat(fpath).st_mtime < (time.time() - (self._retention_days * 86400.0))

        def _should_delete_dir(dpath):
            return os.path.isdir(dpath) and not os.listdir(dpath)

        files = filter(_should_delete_file, glob.glob(f'{self._base_log_dir}/**/*.log'))
        # Type ignore hints due to mypy bugging out and detecting incorrect type...
        for f in files:  # type: ignore
            print(f'Deleting Log File: {f}')
            os.remove(f)  # type: ignore

        dirs = filter(_should_delete_dir, [f'{self._base_log_dir}/{p}' for p in os.listdir(self._base_log_dir)])
        for d in dirs:
            print(f'Deleting Directory: {d}')
            os.rmdir(d)
