import traceback, os, signal
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from multiprocessing.sharedctypes import Value
from ..managers.logmanager import LogManager
from ..lifecycle import Lifecycle

class Task(ABC, Lifecycle):

    def __init__(self, stash: Dict[str, Any], logger: LogManager, args: List[Any], kwargs: Dict[str, Any], restart: bool = False) -> None:
        self._is_failed = Value("i", 0)
        self.stash = stash
        self.logger = logger
        self.args = args
        self.kwargs = kwargs
        self.restart = restart

    def _exec_lifecycle_stage(self, stage) -> None:
        try:
            stage()
        except Exception as e:
            self.is_failed = True
            self.logger.critical(str(e))
            self.logger.critical(traceback.format_exc())

    @property
    def is_failed(self) -> bool:
        return bool(self._is_failed.value)
    
    @is_failed.setter
    def is_failed(self, val: bool) -> None:
        self._is_failed.value = 1 if val else 0

    def run_lifecycle(self) -> None:
        # Bind signal only in new child process
        signal.signal(signal.SIGTERM, lambda *_: self._exec_lifecycle_stage(self.on_abort))
        
        try:
            self.logger._on_create()
            nullfd = os.open(os.devnull, os.O_RDWR)
            os.dup2(nullfd, 1)
            os.dup2(nullfd, 2)

            self._exec_lifecycle_stage(self.on_create)

            if self.restart:
                self._exec_lifecycle_stage(self.on_restart)

            self._exec_lifecycle_stage(self.run)

            if self.is_failed:
                self._exec_lifecycle_stage(self.on_failure)
            else:
                self._exec_lifecycle_stage(self.on_success)
                self.is_failed = False

            self._exec_lifecycle_stage(self.on_destroy)
            os.close(nullfd)
        except Exception:
            print(traceback.format_exc())
            self.is_failed = True
        finally:
            self.logger._on_destroy()

    @abstractmethod
    def run(self) -> None:
        pass