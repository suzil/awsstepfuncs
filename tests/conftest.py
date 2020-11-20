import json
from contextlib import closing, redirect_stdout
from io import StringIO
from typing import Callable

import pytest


@pytest.fixture()
def compile_state_machine(tmp_path) -> Callable:
    def _compile_state_machine(state_machine):
        compiled_path = tmp_path / "state_machine.json"
        state_machine.to_json(compiled_path)
        with compiled_path.open() as fp:
            return json.load(fp)

    return _compile_state_machine


@pytest.fixture()
def capture_stdout():
    def _capture_stdout(simulate_fn):
        with closing(StringIO()) as fp:
            with redirect_stdout(fp):
                simulate_fn()
            return fp.getvalue()

    return _capture_stdout
