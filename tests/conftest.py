import json
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
