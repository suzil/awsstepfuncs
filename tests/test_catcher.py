import contextlib
from contextlib import redirect_stdout
from io import StringIO

from awsstepfuncs import FailState, SucceedState, TaskState
from awsstepfuncs.state_machine import StateMachine


def test_catcher():
    resource = "123"
    task_state = TaskState("Task", resource=resource)
    succeed_state = SucceedState("Success")
    fail_state = FailState("Failure", error="IFailed", cause="I failed!")

    task_state >> succeed_state
    task_state.add_catcher(["States.ALL"], next_state=fail_state)

    state_machine = StateMachine(start_state=task_state)
    assert state_machine.compile() == {
        "StartAt": "Task",
        "States": {
            "Task": {
                "Type": "Task",
                "Next": "Success",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failure"}],
                "Resource": "123",
            },
            "Success": {"Type": "Succeed"},
            "Failure": {"Type": "Fail", "Error": "IFailed", "Cause": "I failed!"},
        },
    }

    def success_mock_fn(_):
        assert True

    def failure_mock_fn(_):
        assert False  # noqa: PT015

    stdouts = {}
    for mock_fn in [success_mock_fn, failure_mock_fn]:
        with contextlib.closing(StringIO()) as fp:
            with redirect_stdout(fp):
                state_machine.simulate(resource_to_mock_fn={resource: mock_fn})
            stdouts[mock_fn] = fp.getvalue()

    assert (
        stdouts[success_mock_fn]
        == """Running Task
Running Success
"""
    )
    assert (
        stdouts[failure_mock_fn]
        == """Running Task
Running Failure
"""
    )
