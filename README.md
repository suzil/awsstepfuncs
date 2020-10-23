# AWS Step Functions SDK

[![python](https://img.shields.io/static/v1?label=python&message=3.8%2B&color=informational&logo=python&logoColor=white)](https://github.com/suzil/aws-step-functions/releases/latest)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![Actions Status](https://github.com/suzil/aws-step-functions/workflows/GH/badge.svg)](https://github.com/suzil/aws-step-functions/actions)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![codecov](https://codecov.io/gh/suzil/aws-step-functions/branch/master/graph/badge.svg?token=PF990VH0YU)](https://codecov.io/gh/suzil/aws-step-functions)
[![Documentation Status](https://readthedocs.org/projects/aws-step-functions/badge/?version=latest)](https://aws-step-functions.readthedocs.io/en/latest/?badge=latest)

WARNING: This project is still a work-in-progress. Buyer beware.

## Installation

This package is available on PyPI:

```sh
$ pip install awsstepfuncs
```


## Usage

```py
from awsstepfuncs import TaskState, PassState, StateMachine

# Define some states
pass_state = PassState(
    "My Pass", comment="Passes its input to its output without performing work"
)
times_two_resource = (
    "arn:aws:lambda:ap-southeast-2:710187714096:function:DivideNumbers"
)
task_state = TaskState(
    "My Task",
    comment="Times two task",
    resource=times_two_resource,
)

# Define a state machine that orchestrates the states
pass_state >> task_state
state_machine = StateMachine(start_state=pass_state)

# Compile the state machine to Amazon States Language
state_machine.to_json("state_machine.json")

# Simulate the state machine by executing it, use mock functions for tasks

def mock_times_two(data):
    data["foo"] *= 2
    return data

state_output = state_machine.simulate(
    state_input={"foo": 5, "bar": 1},
    resource_to_mock_fn={times_two_resource: mock_times_two,
})

assert state_output == {"foo": 10, "bar": 1}
```


## Development

Create a virtual environment:

```sh
$ python -m venv .venv
$ source .venv/bin/activate
```

Install all dependencies:

```sh
$ make install
```

Run lint with:

```sh
$ make lint
```

Run tests with:

```sh
$ make test
```
