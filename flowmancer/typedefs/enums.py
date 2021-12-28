from enum import Enum

class ExecutionState(Enum):
    FAILED = "F"
    PENDING = "P"
    RUNNING = "R"
    DEFAULTED = "D"
    COMPLETED = "C"
    ABORTED = "A"
    NORUN = "N"

class Signal(Enum):
    ABORT = 1
    PAUSE = 2
    PULSE = 3
    REVIVE = 4