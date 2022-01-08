import requests, json
from .observer import Observer

class SlackNotification(Observer):
    def __init__(self, webhook: str) -> None:
        self._webhook = webhook
    
    def update(self) -> None:
        # This Observer only reacts on success/failure
        pass

    def _post_message(self, msg: str) -> None:
        requests.post(
            self._webhook,
            data = json.dumps({ 'text': msg }),
            headers = {'Content-Type': 'application/json'}
        )

    def on_success(self) -> None:
        self._post_message("success")
    
    def on_failure(self) -> None:
        self._post_message("failure")