import contextlib
from contextlib import redirect_stdout
from io import StringIO

from awsstepfuncs import PassState, StateMachine, TaskState


def test_multiple_catchers():
    resource = "123"
    task_state = TaskState("Task", resource=resource)
    timeout_state = PassState("Timeout")
    task_failed_state = PassState("Task Failed")
    task_state.add_catcher(["States.Timeout"], next_state=timeout_state).add_catcher(
        ["States.TaskFailed"], next_state=task_failed_state
    )
    state_machine = StateMachine(start_state=task_state)

    def failure_mock_fn(event, context):
        # Will cause a TaskFailedError
        assert False  # noqa: PT015

    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_machine.simulate(resource_to_mock_fn={resource: failure_mock_fn})
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert stdout == [
        "Starting simulation of state machine",
        "Executing TaskState('Task')",
        "State input: {}",
        "State input after applying input path of $: {}",
        "TaskFailedError encountered in state",
        "Checking for catchers",
        "Found catcher, transitioning to PassState('Task Failed')",
        "State output: {}",
        "Executing PassState('Task Failed')",
        "State input: {}",
        "State input after applying input path of $: {}",
        "Output from applying result path of $: {}",
        "State output after applying output path of $: {}",
        "State output: {}",
        "Terminating simulation of state machine",
    ]
