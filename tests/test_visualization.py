from awsstepfuncs import FailState, PassState, StateMachine, SucceedState, TaskState


def test_visualization():
    resource = "123"
    task_state = TaskState("My task", resource=resource)
    succeed_state = SucceedState("Success")
    pass_state = PassState("Just passing")
    fail_state = FailState("Failure", error="IFailed", cause="I failed!")

    task_state >> succeed_state
    pass_state >> fail_state
    task_state.add_catcher(["States.ALL"], next_state=pass_state)

    state_machine = StateMachine(start_state=task_state)

    def failure_mock_fn(_):
        assert False  # noqa: PT015

    state_machine.simulate(
        resource_to_mock_fn={resource: failure_mock_fn}, show_visualization=True
    )
