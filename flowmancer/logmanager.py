from .typedefs.models import LoggersDefinition
from .loggers.file import FileLogger

class LogManager:
    def __init__(self, task_name: str, logdefs: LoggersDefinition) -> None:
        self._loggers = []
        for tp, detl in logdefs:
            if tp == "file":
                self._loggers.append(FileLogger(task_name, detl))

    def prepare(self) -> None:
        for l in self._loggers:
            l.prepare()

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