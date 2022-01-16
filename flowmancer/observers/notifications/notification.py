from abc import abstractmethod
from typing import Dict
from collections import defaultdict
from ..observer import Observer
from ...typedefs.enums import ExecutionState

class Notification(Observer):

    def update(self) -> None:
        # We don't want notifications to be spammed...
        pass
    
    def _get_state_counts(self) -> Dict[ExecutionState, int]:
        counts = defaultdict(lambda:0)
        for ex in self.executors.values():
            counts[ex.state] += 1
        return counts
    
    @abstractmethod
    def send_notification(self, title: str, msg: str) -> None:
        pass

    def on_create(self) -> None:
        self.send_notification("STARTING", "Initiating Job")

    def on_success(self) -> None:
        self.send_notification("SUCCESS", "Final Status: SUCCESS")
    
    def on_failure(self) -> None:
        self.send_notification("FAILURE", "Final Status: FAILURE")