from .observer import Observer
from ..typedefs.enums import ExecutionState

class Synchro(Observer):

    def update(self) -> None:
        if not self.executors.get_executors_in_state(ExecutionState.PENDING):
            self._root_event.set()