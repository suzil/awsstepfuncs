# AWS Step Functions SDK

[![python](https://img.shields.io/static/v1?label=python&message=3.8%2B&color=informational&logo=python&logoColor=white)](https://github.com/suzil/aws-step-functions/releases/latest)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![Actions Status](https://github.com/suzil/aws-step-functions/workflows/GH/badge.svg)](https://github.com/suzil/aws-step-functions/actions)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
<!-- [![codecov](https://codecov.io/gh/suzil/aws-step-functions/branch/master/graph/badge.svg?token=<add_token_here>)](https://codecov.io/gh/suzil/aws-step-functions) TODO: Enable when the repo is public -->
<!-- TODO: Add ReadTheDocs badge -->


## Installation

This package is available on PyPI:

```sh
$ pip install awsstepfuncs
```


## Usage

```py
from awsstepfuncs import Pass

# Define a series of states
pass_step1 = Pass("Pass 1", description="Passes its input to its output without performing work")
pass_step2 = Pass("Pass 2", description="Here is a second pass step")

# Define a workflow that organizes the states
workflow = pass_step1 >> pass_step2

# Compile the workflow to Amazon States Language
workflow.compile("workflow.json")

# Simulate the workflow by actually running it
workflow.run()
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
