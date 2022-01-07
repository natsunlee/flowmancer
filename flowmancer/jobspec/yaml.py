import yaml
from .jobspec import JobSpec
from ..typedefs.models import JobDefinition

class YAML(JobSpec):
    def load(self, filename: str) -> JobDefinition:
        with open(filename, "r") as f:
            j = yaml.safe_load(f)
        version = j.get("version", 0.1)
        return JobDefinition(**j)

    def dump(self, jdef: JobDefinition, filename: str) -> None:
        with open(filename, "r") as f:
            f.write(yaml.dump(jdef.dict()))