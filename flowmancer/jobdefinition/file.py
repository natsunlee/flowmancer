import os
import re
from typing import Any, Callable, Pattern

import yaml

from . import JobDefinition, SerializableJobDefinition, job_definition

_env_var_matcher = re.compile(r'\$ENV{([^}^{]+)}')
_env_tag_matcher = re.compile(r'[^$]*\$ENV{([^}^{]+)}.*')

# TODO: parameters file
# _param_var_matcher = re.compile(r'\$PARAM{([^}^{]+)}')
# _param_tag_matcher = re.compile(r'[^$]*\$PARAM{([^}^{]+)}.*')


def _build_constructor(var_matcher: Pattern[str]) -> Callable[[Any, Any], str]:
    def _path_constructor(_: Any, node: Any) -> str:
        def replace_fn(match):
            parts = f"{match.group(1)}:".split(":")
            return os.environ.get(parts[0], parts[1])
        return var_matcher.sub(replace_fn, node.value)
    return _path_constructor


@job_definition('yaml')
class YAMLJobDefinition(SerializableJobDefinition):
    def load(self, filename: str) -> JobDefinition:
        yaml.add_implicit_resolver("!envvar", _env_tag_matcher, None, yaml.SafeLoader)
        yaml.add_constructor("!envvar", _build_constructor(_env_var_matcher), yaml.SafeLoader)
        with open(filename, 'r') as f:
            return JobDefinition(**yaml.safe_load(f.read()))

    def dump(self, jdef: JobDefinition, filename: str) -> None:
        with open(filename, 'r') as f:
            f.write(yaml.dump(jdef.dict()))
