import contextlib
import json
from contextlib import redirect_stdout
from io import StringIO

from awsstepfuncs import LambdaState, PassState, StateMachine


def test_task_state(tmp_path):
    pass_state = PassState("Pass", comment="The starting state")
    dummy_resource_uri = (
        "arn:aws:lambda:ap-southeast-2:710187714096:function:DivideNumbers"
    )
    task_state = LambdaState("Lambda", resource_uri=dummy_resource_uri)

    # Define the state machine
    pass_state >> task_state
    state_machine = StateMachine(start_state=pass_state)

    # Check the output from compiling
    compiled_path = tmp_path / "state_machine.json"
    state_machine.compile(compiled_path)
    with compiled_path.open() as fp:
        compiled = json.load(fp)
    assert compiled == {
        "StartAt": pass_state.name,
        "States": {
            pass_state.name: {
                "Comment": pass_state.comment,
                "Type": "Pass",
                "Next": task_state.name,
            },
            task_state.name: {
                "Type": "Task",
                "Resource": dummy_resource_uri,
                "End": True,
            },
        },
    }

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_machine.simulate(
                {dummy_resource_uri: lambda: print(1 / 2)}  # noqa: T001
            )
        stdout = fp.getvalue()

    assert (
        stdout
        == """Running Pass
Passing
Running Lambda
0.5
"""
    )
