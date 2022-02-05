import importlib
from typing import Dict
from ..typedefs.models import LoggerDefinition
from ..loggers.logger import Logger

class LogManager:

    def __init__(self, task_name: str, logdefs: Dict[str, LoggerDefinition]) -> None:
        self._loggers = []
        for detl in logdefs.values():
            LogClass = self.get_logger_class(detl.module, detl.logger)
            self._loggers.append(LogClass(task_name, **detl.kwargs))

    def get_logger_class(self, module: str, logger: str) -> Logger:
        log_class = getattr(importlib.import_module(module), logger)
        if not issubclass(log_class, Logger):
            raise TypeError(f"{module}.{logger} is not an extension of Logger")
        return log_class

    def _on_create(self) -> None:
        for l in self._loggers:
            l.on_create()
    
    def _on_destroy(self) -> None:
        for l in self._loggers:
            l.on_destroy()

    def debug(self, msg: str) -> None:
        for l in self._loggers:
            l.debug(msg)

    def info(self, msg: str) -> None:
        for l in self._loggers:
            l.info(msg)

    def warning(self, msg: str) -> None:
        for l in self._loggers:
            l.warning(msg)

    def error(self, msg: str) -> None:
        for l in self._loggers:
            l.error(msg)

    def critical(self, msg: str) -> None:
        for l in self._loggers:
            l.critical(msg)