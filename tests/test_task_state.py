import contextlib
from contextlib import redirect_stdout
from io import StringIO

from awsstepfuncs import PassState, StateMachine, TaskState


def test_task_state(compile_state_machine):
    pass_state = PassState("Pass", comment="The starting state")
    dummy_resource_uri = (
        "arn:aws:lambda:ap-southeast-2:710187714096:function:DivideNumbers"
    )
    task_state = TaskState("Task", resource_uri=dummy_resource_uri)

    # Define the state machine
    pass_state >> task_state
    state_machine = StateMachine(start_state=pass_state)

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
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

    def mock_fn(data):
        data["foo"] *= 2
        return data

    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate(
                state_input={"foo": 5, "bar": 1},
                resource_to_mock_fn={dummy_resource_uri: mock_fn},
            )
        stdout = fp.getvalue()

    assert state_output == {"foo": 10, "bar": 1}
    assert (
        stdout
        == """Running Pass
Passing
Running Task
"""
    )
