from abc import ABC, abstractmethod
from ..typedefs.models import JobDefinition

class JobSpec(ABC):
    @abstractmethod
    def load(self, filename: str) -> JobDefinition:
        pass

    @abstractmethod
    def dump(self, jdef: JobDefinition, filename: str) -> None:
        pass