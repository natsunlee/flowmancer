import yaml
from pyaml_env import parse_config
from .jobspec import JobSpec
from ..typedefs.models import JobDefinition


class YAML(JobSpec):
    def load(self, filename: str) -> JobDefinition:
        j = parse_config(filename)
        return JobDefinition(**j)

    def dump(self, jdef: JobDefinition, filename: str) -> None:
        with open(filename, "r") as f:
            f.write(yaml.dump(jdef.dict()))
