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
pip install flowmancer
```

NOTE: `flowmancer` supports only Python 3.7 and higher.

## Usage
Let's assume you have a new project with a basic structure like so:
```
my_project
├─ job.yaml
├─ main.py
└─ tasks/
   └─ mytasks.py
```

To use `flowmancer`, you'll need to provide a few things:
* `Task` implementations (`mytasks.py`)
* A job YAML file (`job.yaml`)
* Your main/driver code (`main.py`)

### Tasks
A `flowmancer` task is simply a class that extends the `Task` abstract class, which, at minimum requires that the `run` method be implemented:
```python
from flowmancer import Task, task
import time

@task
class WaitAndSucceed(Task):
    def run(self):
        print("Starting up and sleeping for 5 seconds!")
        time.sleep(5)
        print("Done!")

@task
class FailImmediately(Task):
    def run(self):
        raise RuntimeError("Let this be caught by Flowmancer")
```

Any `print()` or exceptions will write log messages to any configured loggers (zero or more loggers may be defined).

### Job Definition YAML File
This file describes what code to run, in what order, as well as additional add-ons to supplement the job during execution:
```yaml
version: 0.1

tasks:
  # No dependency - run right away
  succeed-task-a:
    task: WaitAndSucceed

  # No dependency - run right away
  succeed-task-b:
    task: WaitAndSucceed

  # Only run if prior 2 tasks complete successfully
  final-fail-task:
    task: FailImmediately
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
    ret = Flowmancer().start()
    sys.exit(ret)
```

### Executing the Job
```bash
python main.py -j ./path/to/job.yaml
```

To run from point-of-failure (if any), if Checkpoint observer is enabled:
```bash
python main.py -r
```
If no prior failure is detected, the job will start as if no `-r` flag were given.
