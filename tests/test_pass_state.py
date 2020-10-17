import contextlib
import json
from contextlib import redirect_stdout
from io import StringIO

from awsstepfuncs import PassState, StateMachine


def test_pass_state(tmp_path):
    pass_state1 = PassState("Pass 1", comment="The starting state")
    pass_state2 = PassState("Pass 2")
    pass_state3 = PassState("Pass 3")

    # Define the state machine
    pass_state1 >> pass_state2 >> pass_state3
    state_machine = StateMachine(start_state=pass_state1)

    # Make sure that the DAG is correctly specified
    assert [state.name for state in state_machine.start_state] == [
        pass_state1.name,
        pass_state2.name,
        pass_state3.name,
    ]

    # Check the output from compiling
    compiled_path = tmp_path / "state_machine.json"
    state_machine.compile(compiled_path)
    with compiled_path.open() as fp:
        compiled = json.load(fp)
    assert compiled == {
        "StartAt": pass_state1.name,
        "States": {
            pass_state1.name: {
                "Comment": pass_state1.comment,
                "Type": "Pass",
                "Next": pass_state2.name,
            },
            pass_state2.name: {
                "Type": "Pass",
                "Next": pass_state3.name,
            },
            pass_state3.name: {
                "Type": "Pass",
                "End": True,
            },
        },
    }

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_machine.simulate()
        stdout = fp.getvalue()

    assert (
        stdout
        == """Running Pass 1
Passing
Running Pass 2
Passing
Running Pass 3
Passing
"""
    )
