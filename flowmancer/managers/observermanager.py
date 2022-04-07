import asyncio
import importlib
from typing import Any, Dict, List, Type

from flowmancer.managers.executormanager import ExecutorManager

from ..observers.observer import Observer
from ..observers.synchro import Synchro
from ..typedefs.models import ObserverDefinition


class ObserverManager:
    def __init__(
        self, observer_def: Dict[str, ObserverDefinition], executors: ExecutorManager, restart: bool = False
    ) -> None:
        root_event = asyncio.Event()

        # Init with default/required observer
        self._observers: List[Observer] = [
            Synchro(root_event=root_event, executors=executors, sleep_time=0.5, restart=restart)
        ]

        for detl in observer_def.values():
            obs_args: Dict[str, Any] = detl.kwargs
            obs_args["root_event"] = root_event
            obs_args["executors"] = executors
            obs_args["sleep_time"] = 0.5
            obs_args["restart"] = restart
            ObsClass = self.get_observer_class(detl.module, detl.observer)
            self._observers.append(ObsClass(**obs_args))

    def get_observer_class(self, module: str, observer: str) -> Type[Observer]:
        obs_class = getattr(importlib.import_module(module), observer)
        if not issubclass(obs_class, Observer):
            raise TypeError(f"{module}.{observer} is not an extension of Observer")
        return obs_class

    def create_tasks(self) -> List[asyncio.Task]:
        return [asyncio.create_task(obs.start()) for obs in self._observers]
