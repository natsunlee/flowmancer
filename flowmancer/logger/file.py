import logging, os

from ..jobspec.schema.v0_1 import FileLoggerDefinition
from .logger import Logger

class FileLogger(Logger):
    def __init__(self, task_name: str, detl: FileLoggerDefinition) -> None:
        os.makedirs(detl.path, exist_ok=True)
        self.filename = f"{detl.path}/{task_name}.log"
        self._level = logging.INFO
    
    def prepare(self) -> None:
        logging.basicConfig(
            filename=self.filename,
            filemode='a+',
            level=self._level,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %I:%M:%S %p %Z'
        )
    
    def debug(self, msg: str) -> None:
        logging.debug(msg)
    def info(self, msg: str) -> None:
        logging.info(msg)
    def warning(self, msg: str) -> None:
        logging.warning(msg)
    def error(self, msg: str) -> None:
        logging.error(msg)
    def critical(self, msg: str) -> None:
        logging.critical(msg)