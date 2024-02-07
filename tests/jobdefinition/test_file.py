import os
from pathlib import Path

import pytest

from flowmancer.jobdefinition.file import YAMLJobDefinition

_sample_valid_yaml_content = """
config:
    name: test_job

tasks:
    first-task:
        task: DoSomething
        parameters:
            init_step: $ENV{INIT_STEP_NAME:startup step}
            run_step: running on and on

    second-task:
        task: SaveVars
        parameters:
            var_a: $ENV{VAL_WITHOUT_DEFAULT}
"""


@pytest.fixture(scope='module')
def valid_yaml(tmp_path_factory) -> Path:
    fpath = tmp_path_factory.mktemp('jobdef') / 'valid.yaml'
    with open(fpath, 'w') as f:
        f.write(_sample_valid_yaml_content)
    return fpath


def test_env_vars_defaults(valid_yaml: Path) -> None:
    loader = YAMLJobDefinition()
    jdef = loader.load(str(valid_yaml))
    assert(jdef.tasks['first-task'].parameters['init_step'] == 'startup step')
    assert(jdef.tasks['second-task'].parameters['var_a'] == '')


def test_env_vars_with_inputs(valid_yaml: Path) -> None:
    os.environ['INIT_STEP_NAME'] = 'custom step name'
    os.environ['VAL_WITHOUT_DEFAULT'] = 'hello world'
    loader = YAMLJobDefinition()
    jdef = loader.load(str(valid_yaml))
    del os.environ['INIT_STEP_NAME']
    del os.environ['VAL_WITHOUT_DEFAULT']
    assert(jdef.tasks['first-task'].parameters['init_step'] == 'custom step name')
    assert(jdef.tasks['second-task'].parameters['var_a'] == 'hello world')
