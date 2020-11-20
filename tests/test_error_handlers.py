from awsstepfuncs import FailState, PassState, StateMachine, TaskState


def test_retrier_zero_max_attempts():
    task_state = TaskState("Task", resource="123").add_retrier(
        ["SomeError"], max_attempts=0
    )
    fail_state = FailState("Fail", error="SomeError", cause="I did it!")
    task_state >> fail_state

    # TODO


def test_catcher():
    task_state = TaskState("Task", resource="123").add_retrier(
        ["SomeError"], max_attempts=0
    )

    # We should end up at `transition_state` because "States.ALL" catches all
    # errors and transitions to `transition_state`
    transition_state = TaskState("Cleanup", resource="456")
    task_state.add_catcher(["States.ALL"], next_state=transition_state)
    task_state.compile() == {
        "Type": "Task",
        "Next": "Fail",
        "Retry": [{"ErrorEquals": ["SomeError"], "MaxAttempts": 0}],
        "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Cleanup"}],
        "Resource": "123",
    }

    another_fail_state = FailState(
        "AnotherFail", error="AnotherError", cause="I did it again!"
    )
    task_state >> another_fail_state


def test_multiple_catchers(capture_stdout):
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

    stdout = capture_stdout(
        lambda: state_machine.simulate(resource_to_mock_fn={resource: failure_mock_fn})
    )

    assert (
        stdout
        == """Starting simulation of state machine
Executing TaskState('Task')
State input: {}
State input after applying input path of $: {}
TaskFailedError encountered in state
Checking for catchers
Found catcher, transitioning to PassState('Task Failed')
State output: {}
Executing PassState('Task Failed')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {}
State output after applying output path of $: {}
State output: {}
Terminating simulation of state machine
"""
    )
