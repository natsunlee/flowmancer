import requests, json, time
from typing import Dict
from collections import defaultdict
from .observer import Observer
from ..typedefs.enums import ExecutionState

class SlackNotification(Observer):

    def __init__(self, webhook: str, msg_interval: float = float("inf")) -> None:
        self._webhook = webhook
        self._checkpoint = 0

        # Default interval is infinite, meaning no messages until after
        # success/failure unless explicitly given a valid interval.
        self._msg_interval = msg_interval
    
    def _get_state_counts(self) -> Dict[ExecutionState, int]:
        counts = defaultdict(lambda:0)
        for ex in self.executors.values():
            counts[ex.state] += 1
        return counts

    def _post_message(self, msg: str) -> None:
        requests.post(
            self._webhook,
            data = json.dumps({ 'text': msg }),
            headers = {'Content-Type': 'application/json'}
        )

    def on_create(self) -> None:
        self._post_message("Initiating Job")

    def update(self) -> None:
        if (time.time() - self._checkpoint) >= self._msg_interval:
            self._checkpoint = time.time()
            counts = self._get_state_counts()
            p = counts[ExecutionState.PENDING]
            r = counts[ExecutionState.RUNNING]
            f = counts[ExecutionState.FAILED]
            self._post_message(f"Pending: {p} | Running: {r} | Failed: {f}")

    def on_success(self) -> None:
        self._post_message("Final Status: SUCCESS")
    
    def on_failure(self) -> None:
        self._post_message("Final Status: FAILURE")