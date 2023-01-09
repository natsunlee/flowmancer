from abc import abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict

from ...typedefs.enums import ExecutionState
from ..observer import Observer


class Notification(Observer):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def update(self) -> None:
        # We don't want notifications to be spammed...
        pass

    def _get_state_counts(self) -> Dict[ExecutionState, int]:
        counts: Dict[ExecutionState, int] = defaultdict(lambda: 0)
        for ex in self.executors.values():
            counts[ex.state] += 1
        return counts

    @abstractmethod
    def send_notification(self, title: str, msg: str) -> None:
        pass

    def on_create(self) -> None:
        self.send_notification(
            "Flowmancer Job Notification: STARTING", f"Job initiated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def on_success(self) -> None:
        self.send_notification(
            "Flowmancer Job Notification: SUCCESS",
            f"Job completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )

    def on_failure(self) -> None:
        self.send_notification(
            "Flowmancer Job Notification: FAILURE", f"Job failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def on_abort(self) -> None:
        self.send_notification(
            "Flowmancer Job Notification: ABORTED", f"Job aborted at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
