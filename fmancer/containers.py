from typing import Dict, Set, Union

from .enums import ExecutionState


class States:
    def __init__(self) -> None:
        self.data: Dict[ExecutionState, Set[str]] = dict()

    def __getitem__(self, k: Union[str, ExecutionState]) -> Set[str]:
        es = ExecutionState(k)
        if es not in self.data:
            self.data[es] = set()
        return self.data[ExecutionState(k)]

    def __str__(self):
        return str(self.data)
