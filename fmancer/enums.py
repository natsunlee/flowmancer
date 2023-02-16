from enum import Enum


class ExecutionState(Enum):
    FAILED = "F"
    PENDING = "P"
    RUNNING = "R"
    DEFAULTED = "D"
    COMPLETED = "C"
    ABORTED = "A"
    SKIP = "S"
    INIT = "_"
