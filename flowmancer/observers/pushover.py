import time, http, urllib
from typing import Dict
from collections import defaultdict
from .observer import Observer
from ..typedefs.enums import ExecutionState

class PushoverNotification(Observer):

    def __init__(self, app_token: str, user_key: str, msg_interval: float = float("inf")) -> None:
        self._app_token = app_token
        self._user_key = user_key
        self._checkpoint = 0

        # Default interval is infinite, meaning no messages until after
        # success/failure unless explicitly given a valid interval.
        self._msg_interval = msg_interval
    
    def _get_state_counts(self) -> Dict[ExecutionState, int]:
        counts = defaultdict(lambda:0)
        for ex in self.executors.values():
            counts[ex.state] += 1
        return counts

    def _send_notification(self, msg: str) -> None:
        conn = http.client.HTTPSConnection("api.pushover.net:443")
        conn.request("POST", "/1/messages.json",
          urllib.parse.urlencode({
            "token": self._app_token,
            "user": self._user_key,
            "message": msg
          }), { "Content-type": "application/x-www-form-urlencoded" })
        conn.getresponse()

    def on_create(self) -> None:
        self._send_notification("Initiating Job")

    def update(self) -> None:
        if (time.time() - self._checkpoint) >= self._msg_interval:
            self._checkpoint = time.time()
            counts = self._get_state_counts()
            p = counts[ExecutionState.PENDING]
            r = counts[ExecutionState.RUNNING]
            f = counts[ExecutionState.FAILED]
            self._send_notification(f"Pending: {p} | Running: {r} | Failed: {f}")

    def on_success(self) -> None:
        self._send_notification("Final Status: SUCCESS")
    
    def on_failure(self) -> None:
        self._send_notification("Final Status: FAILURE")