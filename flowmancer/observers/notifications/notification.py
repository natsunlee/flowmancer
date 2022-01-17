from abc import abstractmethod
from typing import Dict
from datetime import datetime
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
        self.send_notification("Flowmancer Job Notification: STARTING", f"Job initiated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def on_success(self) -> None:
        self.send_notification("Flowmancer Job Notification: SUCCESS", f"Job completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def on_failure(self) -> None:
        self.send_notification("Flowmancer Job Notification: FAILURE", f"Job failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")