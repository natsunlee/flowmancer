import os
from pathlib import Path
from typing import Any, Callable, Dict

import pytest
import yaml

from flowmancer.jobdefinition import JobDefinition, LoadParams
from flowmancer.jobdefinition.file import YAMLJobDefinition


@pytest.fixture(scope='module')
def jobdef_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp('jobdef')


@pytest.fixture(scope='module')
def write_yaml(jobdef_dir: Path) -> Callable[[str, Dict[str, Any], bool], Path]:
    def f(name: str, content: Dict[str, Any], wrap_vals_with_quotes: bool = False) -> Path:
        fpath = jobdef_dir / name
        with open(fpath, 'w') as f:
            if wrap_vals_with_quotes:
                # Regular dump wraps values in quotes...
                f.write(yaml.dump(content, default_flow_style=False))
            else:
                f.write(yaml.safe_dump(content, default_flow_style=False))
        return fpath
    return f


@pytest.fixture(scope='module')
def load_yaml_jobdef(jobdef_dir: Path) -> Callable[[str], JobDefinition]:
    def f(p: str) -> JobDefinition:
        loader = YAMLJobDefinition()
        params = LoadParams(APP_ROOT_DIR=str(jobdef_dir))
        return loader.load(str(jobdef_dir / p), params)
    return f


def test_env_var_defaults(
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    write_yaml('a.yaml', {
        'config': {'name': '$ENV{JOB_NAME:default name}'},
        'tasks': {'test': {'task': '$ENV{TASK_CLASS:Test}'}}
    })
    j = load_yaml_jobdef('a.yaml')
    assert(j.config.name == 'default name')
    assert(j.tasks['test'].task == 'Test')


def test_env_var_with_inputs(
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    write_yaml('a.yaml', {
        'config': {'name': '$ENV{JOB_NAME:default name}'},
        'tasks': {
            'test': {'task': '$ENV{TASK_CLASS:Test}'},
            'notreal': {'task': '$ENV{NOT_REAL}'}
        }
    })
    os.environ['JOB_NAME'] = 'custom step name'
    os.environ['TASK_CLASS'] = 'DoSomething'
    j = load_yaml_jobdef('a.yaml')
    del os.environ['JOB_NAME']
    del os.environ['TASK_CLASS']
    assert(j.config.name == 'custom step name')
    assert(j.tasks['test'].task == 'DoSomething')
    assert(j.tasks['notreal'].task == '')


def test_sys_var(
    jobdef_dir: Path,
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    write_yaml('a.yaml', {
        'tasks': {
            'test': {
                'task': 'Test',
                'parameters': {
                    'app_root_dir': '$SYS{APP_ROOT_DIR}',
                    'notreal': '$SYS{NOTREAL}',
                }
            }
        }
    })
    j = load_yaml_jobdef('a.yaml')
    assert(j.tasks['test'].parameters['app_root_dir'] == str(jobdef_dir))
    assert(j.tasks['test'].parameters['notreal'] == '')


def test_var_as_literal(
    write_yaml: Callable[[str, Dict[str, Any], bool], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    write_yaml('a.yaml', {
        'tasks': {
            'escaped_env': {'task': '$ENV{LITERAL}'},
            'escaped_sys': {'task': '$SYS{LITERAL}'}
        }
    }, True)
    j = load_yaml_jobdef('a.yaml')
    assert(j.tasks['escaped_env'].task == '$ENV{LITERAL}')
    assert(j.tasks['escaped_sys'].task == '$SYS{LITERAL}')


def test_include_order(
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    a = {
        'config': {
            'name': 'a'
        },
        'tasks': {
            'do-something': {
                'task': 'DoSomething'
            }
        }
    }
    b = {
        'config': {
            'name': 'b'
        },
        'tasks': {
            'do-something': {
                'task': 'DoSomethingElse'
            }
        }
    }
    c = {
        'include': [
            '$SYS{APP_ROOT_DIR}/a.yaml',
            '$SYS{APP_ROOT_DIR}/b.yaml'
        ],
        'config': {
            'name': 'c'
        }
    }
    write_yaml('a.yaml', a)
    write_yaml('b.yaml', b)
    write_yaml('c.yaml', c)
    jdef = load_yaml_jobdef('c.yaml')
    assert(jdef.config.name == 'c')
    assert(jdef.tasks['do-something'].task == 'DoSomethingElse')


def test_nested_include(
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    a = {
        'config': {
            'name': 'a'
        },
        'tasks': {
            'do-something': {
                'task': 'DoSomething'
            }
        }
    }
    b = {
        'include': [
            '$SYS{APP_ROOT_DIR}/a.yaml'
        ],
        'config': {
            'name': 'b'
        },
        'tasks': {
            'do-something': {
                'task': 'DoSomethingElse'
            }
        }
    }
    c = {
        'include': [
            '$SYS{APP_ROOT_DIR}/b.yaml'
        ],
        'config': {
            'name': 'c'
        }
    }
    write_yaml('a.yaml', a)
    write_yaml('b.yaml', b)
    write_yaml('c.yaml', c)
    jdef = load_yaml_jobdef('c.yaml')
    assert(jdef.config.name == 'c')
    assert(jdef.tasks['do-something'].task == 'DoSomethingElse')


def test_relative_path_include(
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    a = {'tasks': {'do-something': {'task': 'DoSomething'}}}
    b = {'include': ['./a.yaml'], 'tasks': {'do-something': {'task': 'DoSomethingElse'}}}
    write_yaml('a.yaml', a)
    write_yaml('b.yaml', b)
    jdef = load_yaml_jobdef('b.yaml')
    assert(jdef.tasks['do-something'].task == 'DoSomethingElse')
