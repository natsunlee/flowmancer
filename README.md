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

NOTE: `flowmancer` supports only Python 3.7 and higher.

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
    # All variables should be given type hints and optional vars should be given default values.
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
        # Raise errors to cause tasks to fail and additionally block dependent tasks, if any.
        raise RuntimeError("Let this be caught by Flowmancer")
```

Any `print()` or exceptions will write log messages to any configured loggers (zero or more loggers may be defined).

### Job Definition YAML File
This file describes what code to run, in what order, as well as additional add-ons to supplement the job during execution:
```yaml
version: 0.1

# This entire config block is currently optional, however, it is recommended to at least provide a unique name for each
# Job Definition YAML file, as this name is used for checkpointing jobs in the event of failures.
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
    max_attempts: 3  # Retry uup to 2 times upon failure (1 initial exec + 2 retries = 3 attempts)
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
    # The `start()` method will return a non-zero integer on failure, typically equal to the number of failed tasks.
    ret = Flowmancer().start()

    # Exceptions from tasks will be captured and logged, rather than being raised up to this level. To cause this
    # driver program to fail, either explicitly raise your own error OR call sys.exit.
    if ret:
      raise RuntimeError('Flowmancer job has failed!')

    # Alternatively, instead of crashing w/ an exception, simply exit with a non-zero value.
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

### Complex Parameters
While this is mostly used for `Task` implementations, the details outlined here apply for any built-in and custom `Extension` and `Logger` implementations.

Flowmancer makes heavy use of [Pydantic](https://docs.pydantic.dev/latest/) to validate parameters and ensure that values loaded from the Job Definition are of the appropriate type.

This means that a `Task` can have complex types (including custom models) like:
```python
from enum import Enum
from flowmancer.task import Task, task
from pydantic import BaseModel
from typing import Dict, List

class Protocol(Enum):
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
Coming soon.

### Custom Extensions
Coming soon.
