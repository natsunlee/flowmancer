import logging, os, glob, time
from datetime import datetime
from ..typedefs.models import FileLoggerDefinition
from .logger import Logger

class FileLogger(Logger):
    def __init__(self, task_name: str, detl: FileLoggerDefinition) -> None:
        os.makedirs(detl.path, exist_ok=True)
        self._now = datetime.now()
        ts_str = self._now.strftime("%Y-%m-%d.%H.%M.%S")
        self._level = logging.INFO
        self._retention_days = detl.retention_days
        self._file_prefix = f"{detl.path}/{task_name}."
        self.filepath = f"{self._file_prefix}{ts_str}.log"
    
    def prepare(self) -> None:
        logging.basicConfig(
            filename=self.filepath,
            filemode='a+',
            level=self._level,
            format='%(asctime)s [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %I:%M:%S %p %Z'
        )
    
    def cleanup(self) -> None:
        if self._retention_days < 0:
            return
        files = glob.glob(f"{self._file_prefix}*")
        to_delete = [
            f for f
            in files
            if (os.stat(f).st_mtime < (time.time() - (self._retention_days * 86400.0)))
            and os.path.basename(f) != os.path.basename(self.filepath)
        ]
        for f in to_delete:
            self.info('Deleting Log File: {}'.format(f))
            os.remove(f)
    
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