from abc import ABC, abstractmethod
from ..lifecycle import Lifecycle

class Logger(ABC, Lifecycle):
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