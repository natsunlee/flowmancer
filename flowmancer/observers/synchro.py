from .observer import Observer, _root_event

class Synchro(Observer):
    async def start(self) -> None:
        pending = set(self.executors.values())
        while pending:
            for ex in pending.copy():
                if not ex.is_alive:
                    pending.remove(ex)
            await self.sleep()
        _root_event.set()
    
    def update(self) -> None:
        # This observer is unique - it is responsible for
        # indicating to other observers whether or not
        # all tasks are complete.
        pass