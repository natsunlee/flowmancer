import os
import re
from pathlib import Path
from typing import Any, Union

import yaml

from . import JobDefinition, SerializableJobDefinition, job_definition


def _path_constructor(_: Any, node: Any) -> str:
    def replace_fn(match):
        parts = f"{match.group(1)}:".split(":")
        return os.environ.get(parts[0], parts[1])
    _env_var_matcher = re.compile(r'\$ENV{([^}^{]+)}')
    return _env_var_matcher.sub(replace_fn, node.value)


@job_definition('yaml')
class YAMLJobDefinition(SerializableJobDefinition):
    def load(self, filename: Union[Path, str]) -> JobDefinition:
        _env_tag_matcher = re.compile(r'[^$]*\$ENV{([^}^{]+)}.*')
        yaml.add_implicit_resolver("!envvar", _env_tag_matcher, None, yaml.SafeLoader)
        yaml.add_constructor("!envvar", _path_constructor, yaml.SafeLoader)
        with open(filename, 'r') as f:
            return JobDefinition(**yaml.safe_load(f.read()))

    def dump(self, jdef: JobDefinition, filename: Union[Path, str]) -> None:
        with open(filename, 'r') as f:
            f.write(yaml.dump(jdef.dict()))
