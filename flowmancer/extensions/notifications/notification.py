from abc import abstractmethod
from datetime import datetime

from ...executor import SerializableExecutionEvent
from ..extension import Extension


class Notification(Extension):
    async def update(self, _: SerializableExecutionEvent) -> None:
        # We don't want notifications to be spammed...
        pass

    @abstractmethod
    async def send_notification(self, title: str, msg: str) -> None:
        pass

    async def on_create(self) -> None:
        await self.send_notification(
            "Flowmancer Job Notification: STARTING", f"Job initiated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    async def on_success(self) -> None:
        await self.send_notification(
            "Flowmancer Job Notification: SUCCESS",
            f"Job completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )

    async def on_failure(self) -> None:
        await self.send_notification(
            "Flowmancer Job Notification: FAILURE", f"Job failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    async def on_abort(self) -> None:
        await self.send_notification(
            "Flowmancer Job Notification: ABORTED", f"Job aborted at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
