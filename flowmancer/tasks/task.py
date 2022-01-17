import traceback, os
from multiprocessing.sharedctypes import Value
from abc import ABC, abstractmethod
from ..logmanager import LogManager
from ..lifecycle import Lifecycle

class Task(ABC, Lifecycle):

    def __init__(self, logger: LogManager) -> None:
        self._is_failed = Value("i", 0)
        self.logger = logger

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
        try:
            self.logger.prepare()
            nullfd = os.open(os.devnull, os.O_RDWR)
            os.dup2(nullfd, 1)
            os.dup2(nullfd, 2)

            self._exec_lifecycle_stage(self.on_create)
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
            self.logger.cleanup()

    @abstractmethod
    def run(self) -> None:
        pass