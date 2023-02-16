from typing import Any, Tuple


class Lifecycle:
    __slots__ = tuple()  # type: ignore

    def on_create(self) -> None:
        # Optional lifecycle method
        pass

    def on_restart(self) -> None:
        # Optional lifecycle method
        pass

    def on_success(self) -> None:
        # Optional lifecycle method
        pass

    def on_failure(self) -> None:
        # Optional lifecycle method
        pass

    def on_destroy(self) -> None:
        # Optional lifecycle method
        pass

    def on_abort(self) -> None:
        # Optional lifecycle method
        pass


class AsyncLifecycle:
    __slots__: Tuple[Any] = tuple()  # type: ignore

    async def on_create(self) -> None:
        # Optional lifecycle method
        pass

    async def on_restart(self) -> None:
        # Optional lifecycle method
        pass

    async def on_success(self) -> None:
        # Optional lifecycle method
        pass

    async def on_failure(self) -> None:
        # Optional lifecycle method
        pass

    async def on_destroy(self) -> None:
        # Optional lifecycle method
        pass

    async def on_abort(self) -> None:
        # Optional lifecycle method
        pass
