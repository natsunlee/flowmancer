from abc import ABC, abstractmethod
from typing import Any

from ..lifecycle import Lifecycle


class Logger(ABC, Lifecycle):
    def __init__(self, *_: Any) -> None:
        # To please linters...
        pass

    @abstractmethod
    def debug(self, msg: str) -> None:
        pass

    @abstractmethod
    def info(self, msg: str) -> None:
        pass

    @abstractmethod
    def warning(self, msg: str) -> None:
        pass

    @abstractmethod
    def error(self, msg: str) -> None:
        pass

    @abstractmethod
    def critical(self, msg: str) -> None:
        pass
