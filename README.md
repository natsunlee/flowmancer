# Flowmancer

[![pypi-version](https://img.shields.io/pypi/v/flowmancer?style=flat-square)](https://pypi.org/project/flowmancer)
[![python-version](https://img.shields.io/badge/dynamic/json?query=info.requires_python&label=python&url=https%3A%2F%2Fpypi.org%2Fpypi%2Fflowmancer%2Fjson&style=flat-square)](https://pypi.org/project/flowmancer)
[![license](https://img.shields.io/github/license/natsunlee/flowmancer?style=flat-square)](LICENSE)
[![circle-ci](https://img.shields.io/circleci/build/github/natsunlee/flowmancer?style=flat-square)](https://app.circleci.com/pipelines/github/natsunlee/flowmancer)
[![coveralls](https://img.shields.io/coveralls/github/natsunlee/flowmancer?style=flat-square)](https://coveralls.io/github/natsunlee/flowmancer?branch=main)
[![pypi-downloads](https://img.shields.io/pypi/dm/flowmancer?style=flat-square)](https://pypistats.org/packages/flowmancer)
[![Ko-Fi](https://img.shields.io/badge/Support%20Me%20On%20Ko--fi-F16061?style=flat-square&logo=ko-fi&logoColor=white)](https://ko-fi.com/natsunlee)

Flowmancer aims to help you do *things* in a sequential or parallel manner. It enables you to write tasks in Python, describe their order, then execute them with as little effort as possible.

But why do I need this? Couldn't I just write my own Python code to do *stuff*?

You certainly could!

Though Flowmancer provides gives you a head-start to building your custom processes with optional add-ons for logging, checkpoint/restarts in the event of failures, or even custom task observers to do...things while your things do things!

## Installation
Simply install the `flowmancer` package with:
```bash
pip3 install flowmancer
```

NOTE: `flowmancer` supports only Python 3.8.1 and higher. Generally speaking, support will follow the [status of Python versions](https://devguide.python.org/versions/), though minimum supported version may occasionally be higher due to requirements of critical dependencies.

## Basic Usage
Let's assume you have a new project with a basic structure like so:
```
my_project
├─ job.yaml
├─ main.py
└─ tasks/
   ├─ __init__.py
   └─ mytasks.py
```

To use `flowmancer`, you'll need to provide a few things:
* `Task` implementations (`mytasks.py`)
* A job YAML file (`job.yaml`)
* Your main/driver code (`main.py`)

### Tasks
By default, Flowmancer recursively searches in the `./tasks` directory (relative to where `Flowmancer()` is initialized - in this case, `main.py`) for `Task` implementations decorated with `@task`. See the Advanced Usage section for details on how to add other directories or packages that contain `Task` implementations.

A `flowmancer` task is simply a class that extends the `Task` abstract class, which, at minimum requires that the `run` method be implemented:
```python
import time
from flowmancer.task import Task, task

@task
class WaitAndSucceed(Task):
    # All variables should be given type hints and optional vars should be given default
    # values.
    my_required_string_var: str
    my_optional_int_var: int = 5

    def run(self):
        # Store string input var in the shared dictionary accessible by other tasks.
        self.shared_dict["my_var"] = f"Hello from: {self.my_required_string_var}!"

        # Sleep for seconds defined by input var (using default of 5).
        print(f"Starting up and sleeping for {self.my_optional_int_var} seconds!")
        time.sleep(self.my_optional_int_var)
        print("Done!")

@task
class ImmediatelySucceed(Task):
    def run(self):
        # Print statements will automatically be sent to configured loggers.
        print("Success!")

@task
class FailImmediately(Task):
    def run(self):
        print(f"Printing `my_var` value: {self.shared_dict['my_var']}")
        # Raise errors to cause tasks to fail and additionally block dependent tasks,
        # if any.
        raise RuntimeError("Let this be caught by Flowmancer")
```

Any `print()` or exceptions will write log messages to any configured loggers (zero or more loggers may be defined).

### Job Definition YAML File
This file describes what code to run, in what order, as well as additional add-ons to supplement the job during execution:
```yaml
version: 0.1

# This entire config block is currently optional, however, it is recommended to at least
# provide a unique name for each Job Definition YAML file, as this name is used for
# checkpointing jobs in the event of failures.
config:
  name: 'my-flowmancer-job'

tasks:
  # No dependency - run right away
  # Add `parameters` key-value pairs for any required and optional task variables.
  succeed-task-a:
    task: WaitAndSucceed
    parameters:
      my_required_string_var: "My First Task!"

  # No dependency - run right away
  succeed-task-b:
    task: ImmediatelySucceed

  # Only run if prior 2 tasks complete successfully
  final-fail-task:
    task: FailImmediately
    max_attempts: 3  # Retry up to 2 times on failure (1st exec + 2 retries = 3 attempts)
    dependencies:
      - succeed-task-a
      - succeed-task-b
```

### Driver
The driver is super simple and simply requires running an instance of `Flowmancer`
```python
# main.py
import sys
from flowmancer import Flowmancer

if __name__ == '__main__':
    # The `start()` method will return a non-zero integer on failure, typically equal to
    # the number of failed tasks.
    ret = Flowmancer().start()

    # Exceptions from tasks will be captured and logged, rather than being raised up to
    # this level. To cause this driver program to fail, do one of 3 things:

    # Explicitly raise your own error.
    if ret:
      raise RuntimeError('Flowmancer job has failed!')

    # Set optional param `raise_exception_on_failure` to `True in the above `.start()`
    # call like so:
    # Flowmancer().start(raise_exception_on_failure=True)

    # Alternatively, instead of crashing w/ an exception, exit with a non-zero value.
    # sys.exit(ret)
```

### Executing the Job
```bash
python3 main.py -j ./path/to/job.yaml
```

To run from point-of-failure (if any):
```bash
python3 main.py -j ./path/to/job.yaml -r
```
If no prior failure is detected, the job will start as if no `-r` flag were given.

Note that the job definition must still be provided with the `-r` flag.

## Advanced Usage

### Optional Configurations
In the `config` block of the Job Definition, the following optional parameters may be given:
|Parameter|Type|Default Value|Description|
|---|---|---|---|
|name|str|'flowmancer'|Name/identifier for Job Definition. Used for saving checkpoints used for job restarts in the event of a failure.|
|max_concurrency|int|0|Maximum number tasks that can run in parallel. If 0 or less, then there is no limit.|
|extension_directories|List[str]|[]|List of paths, either absolute or relative to driver `.py` file, that contain any `@task`, `@logger`, or `@extension` decorated classes to make accessible to Flowmancer. The `./task`, `./extensions`, and `./loggers` directories are ALWAYS checked by default.|
|extension_packages|List[str]|[]|List of installed Python packages that contain `@task`, `@logger`, or `@extension` decorated classes to make accessible to Flowmancer.|
|synchro_interval_seconds|float|0.25|Core execution loop interval for waking and checking status of tasks and whether loggers/extensions/checkpointer should trigger.|
|loggers_interval_seconds|float|0.25|Interval in seconds to wait before emitting log messages to configured `Logger` instances.|
|extensions_interval_seconds|float|0.25|Interval in seconds to wait before emitting state change information to configured `Extension` instances.|
|checkpointer_interval_seconds|float|10.0|Interval in seconds to wait before writing checkpoint information to the configured `Checkpointer`.|

For example:
```yaml
config:
  name: 'most-important-job'
  max_concurrency: 20
  extension_directories:
    - ./client_implementations
    - /opt/flowmancer/tasks
  extension_packages:
    - internal_flowmancer_package
```

### Include YAML Files
An optional `include` block may be defined in the Job Definition in order to merge multiple Job Definition YAML files.
YAML files are provided in a list and processed in the order given, with the containing YAML being processed last.

For example:
```yaml
# <app_root_dir>/jobdefs/template.yaml
config:
  name: generic-template

tasks:
  do-something:
    task: DoSomething
    parameters:
      some_required_param: I am a required string parameter
```

```yaml
# <app_root_dir>/jobdefs/cleanup_addon.yaml
include:
  - ./jobdefs/template.yaml

tasks:
  cleanup:
    task: Cleanup
    dependencies:
      - do-something
```

```yaml
# <app_root_dir>/jobdefs/complete.yaml
config:
  name: complete-job

include:
  - ./jobdefs/cleanup_addon.yaml

tasks:
  do-something:
    task: Do Something
    parameters:
      added_optional_param: 99
```

Loading the `complete.yaml` job definition will result in a YAML equivalent to:
```yaml
config:
  name: complete-job

tasks:
  do-something:
    task: Do Something
    parameters:
      some_required_param: I am a required string parameter
      added_optional_param: 99
```

> :warning: Array values are **NOT** merged like dictionaries are. Any array values (and therfore any nested structures) within them will be replaced if modified in a later YAML.

Additionally, the above example could have all `include` values in the `complete.yaml` file and the `include` block removed from `cleanup_addon.yaml`:
```yaml
# <app_root_dir>/jobdefs/complete.yaml
config:
  name: complete-job

# As with most paths in Job Definition, paths to `include` YAML files are relative to
# `.py` file where the `.start()` method for Flowmancer is invoked.
include:
  - ./jobdefs/template.yaml
  - ./jobdefs/cleanup_addon.yaml

tasks:
  do-something:
    task: Do Something
    parameters:
      added_optional_param: 99
```

The `include` values are processed in order and results in the same outcome as the original example.

### Changing Default File Logger Directory
The Job Definition accepts an optional `loggers` section, which if left empty will default to using a `FileLogger` with default settings.
To utilize the default `FileLogger`, but with a different configuration, explicitly provide the `loggers` block:
```yaml
loggers:
  my-file-logger:
    logger: FileLogger
    parameters:
      # NOTE: this path is relative to the `.py` file where `Flowmancer().start()` is invoked.
      base_log_dir: ./my_custom_log_dir  # ./logs is the default, if omitted.
      retention_days: 3  # 10 is the default, if omitted.
```

### Complex Parameters
While this is mostly used for `Task` implementations, the details outlined here apply for any built-in and custom `Extension` and `Logger` implementations.

Flowmancer makes heavy use of [Pydantic](https://docs.pydantic.dev/latest/) to validate parameters and ensure that values loaded from the Job Definition are of the appropriate type.

This means that a `Task` can have complex types (including custom models) like:
```python
from enum import Enum
from flowmancer.task import Task, task
from pydantic import BaseModel
from typing import Dict, List

class Protocol(str, Enum):
    HTTP: 'HTTP'
    HTTPS: 'HTTPS'

class APIDetails(BaseModel):
    protocol: Protocol = Protocol.HTTPS
    base_url: str
    endpoint: str

@task
class DownloadDataFromRestApi(Task):
    api_details: APIDetails
    target_dir: str
    target_filename: str = 'data.json'

    def run(self) -> None:
        url = f'{self.api_details.protocol}://{self.api_details.base_url}/{self.api_details.endpoint}'
        # Continued implementation...
```

And the Job Definition snippet for this task might be:
```yaml
tasks:
  download-file-one:
    task: DownloadDataFromRestApi
    parameters:
      api_details:
        # We leave out `protocol` because we want to just use the default `HTTPS` value.
        base_url: www.some_data_api.com
        endpoint: /v1/data/weather/today
      target_dir: /data/todays_weather
      # Override the default `target_filename` value given in the class implementation.
      target_filename: weather.json
```

### Task Lifecycle Methods
In addition to the required `run` method, an implementation of `Task` may optionally include the following methods:
|Method|Required|Order|Description|
|---|---|---|---|
|on_create|No|1|First method executed when a task is released for execution. Note that a task is not considered "created" until it enters the `RUNNING` state.|
|on_restart|No|2|Executed only if a task is running from the result of a recovery from `FAILED` state. If a task was failed in `DEFAULTED` state, this method will not be executed.|
|run|Yes|3|Always required and always executed once task is in `RUNNING` state, unless prior lifecycle methods have failed.|
|on_success|No|4|Executed only if `run` method ends in success.|
|on_failure|No|5|Executed only if `run` method ends in failure/exception.|
|on_destroy|No|6|Always executed after all other lifecycle methods.|
|on_abort|No|-|Executed when `SIGINT` signal is sent to tasks/Flowmancer.|

Just as with `run`, all lifecycle methods have access to `self.shared_dict` and any parameters.

### Custom Loggers
Custom implementations of the `Logger` may be provided to Flowmancer to either replace OR write to in addition to the default `FileLogger`.

A custom implementation must extend the `Logger` class, be decorated with the `logger` decorator, and implement the async `update` method at minimum:
```python
@logger
import json
import requests
from flowmancer.loggers.logger import Logger, logger
from flowmancer.eventbus.log import LogEndEvent, LogStartEvent, LogWriteEvent, SerializableLogEvent

class SlackMessageLogger(Logger):
    webhook: str

    def _post_to_slack(self, msg: str) -> None:
        requests.post(
            self.webhook,
            data=json.dumps({'text': title, 'attachments': [{'text': msg}]}),
            headers={'Content-Type': 'application/json'},
        )

    async def update(self, evt: SerializableLogEvent) -> None:
        # The `LogStartEvent` and `LogEndEvent` events only have a `name` property.
        if isinstance(evt, LogStartEvent):
            self._post_to_slack(f'[{evt.name}] START: Logging is beginning')
        elif isinstance(evt, LogEndEvent):
            self._post_to_slack(f'[{evt.name}] END: Logging is ending')
        # The `LogWriteEvent` additionally has `severity` and `message` properties.
        elif isinstance(evt, LogWriteEvent):
            self._post_to_slack(f'[{evt.name}] {evt.severity.value}: {evt.message}')
```

The `Logger` implementation may also have the following optional `async` lifecycle methods:
* `on_create`
* `on_restart`
* `on_success`
* `on_failure`
* `on_destroy`
* `on_abort`

To incorporate your custom `Logger` into Flowmancer, ensure that it exists in a module either in `./loggers` or in a module listed in `config.extension_directories` in the Job Definition.

This allows it to be provided in the `loggers` section of the Job Definition.
> :warning: Providing the `loggers` section will remove the default logger (`FileLogger`) from your job's configuration.
> If you want to add your custom logger alongside the default logger, the `FileLogger` must explicitly be configured.

```yaml
loggers:
  # Load the default logger with default parameters
  default-logger:
    logger: FileLogger

  # Custom logger implementation
  slack-logger:
    logger: SlackMessageLogger
    parameters:
      webhook: https://some.webhook.url
```

### Custom Extensions
Coming soon.

### Custom Checkpointers
Custom implementations of the `Checkpointer` may be provided to Flowmancer to replace the default `FileCheckpointer`.
> :warning: Unlike loggers and extensions, only one checkpointer can be configured per Job Definition.

A custom implementation must extend the `Checkpointer` class, be decorated with the `checkpointer` decorator, and implement the async `write_checkpoint`, `read_checkpoint`, and `clear_checkpoints` methods at minimum. It may also optinoally implement async lifecycle methods, similar to [Custom Loggers](#custom-loggers):
```python
from .checkpointer import CheckpointContents, Checkpointer, NoCheckpointAvailableError, checkpointer

@checkpointer
class DatabaseCheckpointer(Checkpointer):
    host: str
    port: int
    username: str
    password: str

    def write_checkpoint(self, name: str, content: CheckpointContents) -> None:
        # Store checkpoint state - must be able to store contents of
        # `CheckpointContents` in a way that it can be reconstructed later.

    def read_checkpoint(self, name: str) -> CheckpointContents:
        # Recall checkpoint state - reconstruct and return `CheckpointContents`
        # if exists for `name`. Otherwise raise `NoCheckpointAvailableError`
        # to indicate no valid checkpoint exists to restart from.

    def clear_checkpoint(self, name: str) -> None:
        # Remove checkpoint state for `name`.
```

To incorporate your custom `Checkpointer` into Flowmancer, ensure that it exists in a module either in `./extensions` or in a module listed in `config.extension_directories` in the Job Definition.

This allows it to be provided in the `checkpointer` section of the Job Definition:
```yaml
checkpointer:
  checkpointer: DatabaseCheckpointer
  parameters:
    host: something
    port: 9999
    username: user
    password: 1234
```
