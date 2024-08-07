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
    assert(j.tasks['test'].variant == 'Test')


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
    assert(j.tasks['test'].variant == 'DoSomething')
    assert(j.tasks['notreal'].variant == '')


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
    assert(j.tasks['escaped_env'].variant == '$ENV{LITERAL}')
    assert(j.tasks['escaped_sys'].variant == '$SYS{LITERAL}')


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
    assert(jdef.tasks['do-something'].variant == 'DoSomethingElse')


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
    assert(jdef.tasks['do-something'].variant == 'DoSomethingElse')


def test_relative_path_include(
    write_yaml: Callable[[str, Dict[str, Any]], Path],
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    a = {'tasks': {'do-something': {'variant': 'DoSomething'}}}
    b = {'include': ['./a.yaml'], 'tasks': {'do-something': {'variant': 'DoSomethingElse'}}}
    write_yaml('a.yaml', a)
    write_yaml('b.yaml', b)
    jdef = load_yaml_jobdef('b.yaml')
    assert(jdef.tasks['do-something'].variant == 'DoSomethingElse')


def test_aliases(
    jobdef_dir: Path,
    load_yaml_jobdef: Callable[[str], JobDefinition]
) -> None:
    yaml_str = (
        'aliases:\n'
        '  a: &a hello world\n'
        '  b: &b [1, 2, 3]\n'
        '  c: &c\n'
        '    msg: power overwhelming\n'
        '    numbers: [0]\n'
        'tasks:\n'
        '  do-something:\n'
        '    task: DoSomething\n'
        '    parameters:\n'
        '      msg: *a\n'
        '      numbers: *b\n'
        '  do-another-thing:\n'
        '    variant: DoSomething\n'
        '    parameters:\n'
        '      <<: *c\n'
        '      numbers: [999, 9999, 99999]\n'
    )
    with open(jobdef_dir / 'a.yaml', 'w') as f:
        f.write(yaml_str)
    jdef = load_yaml_jobdef('a.yaml')
    assert jdef.tasks['do-something'].parameters['msg'] == 'hello world'
    assert jdef.tasks['do-something'].parameters['numbers'] == [1, 2, 3]
    assert jdef.tasks['do-another-thing'].parameters['msg'] == 'power overwhelming'
    assert jdef.tasks['do-another-thing'].parameters['numbers'] == [999, 9999, 99999]
