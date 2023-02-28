from __future__ import annotations

import signal
import sys
import traceback
from abc import ABC, abstractmethod
from multiprocessing.managers import DictProxy
from multiprocessing.sharedctypes import Value
from queue import Queue
from typing import Any, Dict, Iterable, Optional, TextIO, Union, cast

from .lifecycle import Lifecycle
from .loggers import (LogEndEvent, LogStartEvent, LogWriteEvent,
                      SerializableLogEvent, Severity)

_task_classes = dict()


def task(t: type[Task]):
    if not issubclass(t, Task):
        raise TypeError(f'Must extend `Task` type: {t.__name__}')
    _task_classes[t.__name__] = t
    return t


class LogWriter:
    __slots__ = ("q", "n")

    def __init__(self, n: str, q: Optional[Queue[Any]] = None) -> None:
        self.n = n
        self.q = q
        if q:
            q.put(SerializableLogEvent.serialize(LogStartEvent(name=self.n)))

    def write(self, m: str) -> None:
        if self.q:
            self.q.put(SerializableLogEvent.serialize(LogWriteEvent(name=self.n, severity=Severity.INFO, message=m)))

    def writelines(self, mlist: Iterable[str]) -> None:
        for m in mlist:
            self.write(m)

    def flush(self) -> None:
        # Need to imitate file-like object for redirecting stdout
        pass

    def close(self) -> None:
        if self.q:
            self.q.put(SerializableLogEvent.serialize(LogEndEvent(name=self.n)))


class Task(ABC, Lifecycle):
    __slots__ = ("_log_queue", "shared_dict")

    def __init__(
        self,
        name: str,
        log_queue: Optional[Queue[Any]] = None,
        shared_dict: Optional[Union[DictProxy[str, Any], Dict[str, Any]]] = None
    ) -> None:
        self.name = name
        self.shared_dict = shared_dict if shared_dict is not None else dict()
        self._log_queue = log_queue
        self._is_failed = Value("i", 0)

    @property
    def is_failed(self) -> bool:
        return bool(self._is_failed.value)  # type: ignore

    @is_failed.setter
    def is_failed(self, val: bool) -> None:
        self._is_failed.value = 1 if val else 0  # type: ignore

    def _exec_lifecycle_stage(self, stage) -> None:
        try:
            stage()
        except Exception as e:
            self.is_failed = True
            print(e)

    def run_lifecycle(self) -> None:
        # Bind signal only in new child process
        signal.signal(signal.SIGTERM, lambda *_: self._exec_lifecycle_stage(self.on_abort))
        writer = cast(TextIO, LogWriter(self.name, self._log_queue))

        try:
            sys.stdout = writer
            sys.stderr = writer

            self._exec_lifecycle_stage(self.on_create)

            # if self.restart:
            #    self._exec_lifecycle_stage(self.on_restart)

            self._exec_lifecycle_stage(self.run)

            if self.is_failed:
                self._exec_lifecycle_stage(self.on_failure)
            else:
                self._exec_lifecycle_stage(self.on_success)
                self.is_failed = False

            self._exec_lifecycle_stage(self.on_destroy)
        except Exception:
            print(traceback.format_exc())
            self.is_failed = True
        finally:
            writer.close()

    @abstractmethod
    def run(self) -> None:
        pass
