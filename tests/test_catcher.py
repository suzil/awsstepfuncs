import contextlib
from contextlib import redirect_stdout
from io import StringIO

from awsstepfuncs import FailState, PassState, StateMachine, SucceedState, TaskState


def test_catcher():
    resource = "123"
    task_state = TaskState("Task", resource=resource)
    succeed_state = SucceedState("Success")
    pass_state = PassState("Pass")
    fail_state = FailState("Failure", error="IFailed", cause="I failed!")

    task_state >> succeed_state
    pass_state >> fail_state
    task_state.add_catcher(["States.ALL"], next_state=pass_state)

    state_machine = StateMachine(start_state=task_state)
    assert state_machine.compile() == {
        "StartAt": "Task",
        "States": {
            "Task": {
                "Type": "Task",
                "Next": "Success",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Pass"}],
                "Resource": "123",
            },
            "Success": {"Type": "Succeed"},
            "Pass": {"Type": "Pass", "Next": "Failure"},
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
            stdouts[mock_fn] = [line for line in fp.getvalue().split("\n") if line]

    assert stdouts[success_mock_fn] == [
        "Starting simulation of state machine",
        "Running TaskState('Task')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        'Output from applying result path of "$": {}',
        'State output after applying output path of "$": {}',
        "State output: {}",
        "Running SucceedState('Success')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        'State output after applying output path of "$": {}',
        "State output: {}",
        "Terminating simulation of state machine",
    ]
    assert stdouts[failure_mock_fn] == [
        "Starting simulation of state machine",
        "Running TaskState('Task')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        "Error encountered in state, checking for catchers",
        "Found catcher, transitioning to PassState('Pass')",
        "State output: {}",
        "Running PassState('Pass')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        'Output from applying result path of "$": {}',
        'State output after applying output path of "$": {}',
        "State output: {}",
        "Running FailState('Failure', error='IFailed', cause='I failed!')",
        "State input: {}",
        "State output: {}",
        "Terminating simulation of state machine",
    ]
