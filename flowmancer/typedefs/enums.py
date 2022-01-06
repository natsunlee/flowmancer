from enum import Enum

class ExecutionState(Enum):
    FAILED = "F"
    PENDING = "P"
    RUNNING = "R"
    DEFAULTED = "D"
    COMPLETED = "C"
    ABORTED = "A"
    SKIP = "S"

class Signal(Enum):
    ABORT = 0
    PAUSE = 1
    PULSE = 2
    REVIVE = 3

class LogLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4