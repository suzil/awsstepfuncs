from awsstepfuncs import FailState, PassState, StateMachine, SucceedState, TaskState


def test_retrier_zero_max_attempts():
    task_state = TaskState("Task", resource="123").add_retrier(
        ["SomeError"], max_attempts=0
    )
    fail_state = FailState("Fail", error="SomeError", cause="I did it!")
    task_state >> fail_state

    # TODO


def test_compile_retrier_and_catcher():
    task_state = TaskState("Task", resource="123").add_retrier(
        ["SomeError"], max_attempts=0
    )

    transition_state = TaskState("Cleanup", resource="456")
    task_state.add_catcher(["States.ALL"], next_state=transition_state)
    assert task_state.compile() == {
        "Type": "Task",
        "End": True,
        "Retry": [{"ErrorEquals": ["SomeError"], "MaxAttempts": 0}],
        "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Cleanup"}],
        "Resource": "123",
    }


def test_catcher(capture_stdout):
    resource = "123"
    task_state = TaskState("Task", resource=resource)
    succeed_state = SucceedState("Success")
    pass_state = PassState("Pass")
    fail_state = FailState("Failure", error="IFailed", cause="I failed!")
    task_state >> succeed_state
    pass_state >> fail_state
    task_state.add_catcher(["States.ALL"], next_state=pass_state)
    state_machine = StateMachine(start_state=task_state)

    def failure_mock_fn(event, context):
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
Found catcher, transitioning to PassState('Pass')
State output: {}
Executing PassState('Pass')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {}
State output after applying output path of $: {}
State output: {}
Executing FailState('Failure', error='IFailed', cause='I failed!')
State input: {}
FailStateError encountered in state
Checking for catchers
State output: {}
Terminating simulation of state machine
"""
    )

    # Test no catcher matched
    task_state.catchers.pop()  # Remove States.ALL catcher
    task_state.add_catcher(["Timeout"], next_state=pass_state)
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
No catchers were matched
State output: {}
Terminating simulation of state machine
"""
    )


def test_multiple_catchers(capture_stdout):
    resource = "123"
    task_state = TaskState("Task", resource=resource)
    timeout_state = PassState("Timeout")
    task_failed_state = PassState("Task Failed")
    task_state.add_catcher(["States.Timeout"], next_state=timeout_state).add_catcher(
        ["States.TaskFailed"], next_state=task_failed_state
    )
    state_machine = StateMachine(start_state=task_state)
    assert state_machine.compile() == {
        "StartAt": "Task",
        "States": {
            "Task Failed": {"Type": "Pass", "End": True},
            "Timeout": {"Type": "Pass", "End": True},
            "Task": {
                "Type": "Task",
                "End": True,
                "Catch": [
                    {"ErrorEquals": ["States.Timeout"], "Next": "Timeout"},
                    {"ErrorEquals": ["States.TaskFailed"], "Next": "Task Failed"},
                ],
                "Resource": "123",
            },
        },
    }

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
