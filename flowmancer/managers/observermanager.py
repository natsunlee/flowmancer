import importlib, asyncio
from typing import List
from ..typedefs.models import ObserverDefinition
from ..observers.observer import Observer

class ObserverManager:

    def __init__(self, observer_def: ObserverDefinition) -> None:
        self._observers = []
        for detl in observer_def.values():
            ObsClass = self.get_observer_class(detl.module, detl.observer)
            self._observers.append(ObsClass(**detl.kwargs))
    
    def get_observer_class(self, module: str, observer: str) -> Observer:
        obs_class = getattr(importlib.import_module(module), observer)
        if not issubclass(obs_class, Observer):
            raise TypeError(f"{module}.{observer} is not an extension of Observer")
        return obs_class
    
    def create_tasks(self) -> List[asyncio.Task]:
        return [
            asyncio.create_task(Observer.init_synchro())
        ] + [
            asyncio.create_task(obs.start())
            for obs in self._observers
        ]