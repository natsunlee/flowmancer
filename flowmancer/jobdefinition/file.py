import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Pattern, Union

import yaml

from . import JobDefinition, LoadParams, SerializableJobDefinition, job_definition


def _merge(a: Any, b: Any) -> None:
    for k in b.keys():
        if k in a and isinstance(a[k], dict) and isinstance(b[k], dict):
            _merge(a[k], b[k])
        else:
            a[k] = b[k]


def _build_path_constructor(var_matcher: Pattern[str], val_dict: Dict[str, Any]) -> Callable:
    def _path_constructor(_: yaml.SafeLoader, node: yaml.Node) -> str:
        def replace_fn(match):
            parts = f"{match.group(1)}:".split(":")
            return val_dict.get(parts[0], parts[1])
        return var_matcher.sub(replace_fn, node.value)
    return _path_constructor


@job_definition('yaml')
class YAMLJobDefinition(SerializableJobDefinition):
    def load(
        self, filename: Union[Path, str], params: LoadParams = LoadParams(), vars: Optional[Dict[str, str]] = None
    ) -> JobDefinition:
        # Add constructor for built-in vars
        sys_tag = re.compile(r'[^$]*\$SYS{([^}^{]+)}.*')
        sys_var = re.compile(r'\$SYS{([^}^{]+)}')
        yaml.add_implicit_resolver("!sysvar", sys_tag, None, yaml.SafeLoader)
        yaml.add_constructor("!sysvar", _build_path_constructor(sys_var, dict(params)), yaml.SafeLoader)

        # Add constructor for env vars
        env_tag = re.compile(r'[^$]*\$ENV{([^}^{]+)}.*')
        env_var = re.compile(r'\$ENV{([^}^{]+)}')
        yaml.add_implicit_resolver("!envvar", env_tag, None, yaml.SafeLoader)
        yaml.add_constructor("!envvar", _build_path_constructor(env_var, dict(os.environ)), yaml.SafeLoader)

        # Add constructor for input vars
        input_tag = re.compile(r'[^$]*\$VAR{([^}^{]+)}.*')
        input_var = re.compile(r'\$VAR{([^}^{]+)}')
        yaml.add_implicit_resolver("!inputvar", input_tag, None, yaml.SafeLoader)
        yaml.add_constructor("!inputvar", _build_path_constructor(input_var, vars or dict()), yaml.SafeLoader)

        def process_includes(jdef, merged, seen):
            for p in jdef.get('include', []):
                if not p.startswith('/'):
                    p = os.path.abspath(os.path.join(params.APP_ROOT_DIR, p))
                if p in seen:
                    raise RuntimeError(f'JobDef YAML file has already been processed once: {p}')
                seen.add(p)
                with open(p, 'r') as f:
                    cur = yaml.safe_load(f.read())
                    process_includes(cur, merged, seen)
            _merge(merged, jdef)

        merged: Dict[str, Any] = dict()
        with open(filename, 'r') as f:
            process_includes(yaml.safe_load(f.read()), merged, set())

        # Now that YAML processing is complete, drop the top-level `aliases` section, if any. This allows for having a
        # free-form section for declaring aliases without modifying `JobDefinition` to either add such a section (which
        # would probably be useless for most non-YAML implementations) or setting `extra` to `True`.
        if 'aliases' in merged:
            del merged['aliases']

        return JobDefinition(**merged)

    def dump(self, jdef: JobDefinition, filename: Union[Path, str]) -> None:
        with open(filename, 'r') as f:
            f.write(yaml.dump(jdef.model_dump()))
