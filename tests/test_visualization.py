"""Visualization tests.

Because the GraphViz animations are non-deterministic, there's not really a good
way to test that the output visualization is as-expected (such as comparing
binaries). Therefore, these tests just check if the file gets created and that
there are no runtime exceptions when running the code.
"""

from awsstepfuncs import (
    AndChoice,
    ChoiceRule,
    ChoiceState,
    FailState,
    NotChoice,
    PassState,
    StateMachine,
    SucceedState,
    TaskState,
    VariableChoice,
)


def test_visualization(tmp_path):
    resource = "123"
    task_state = TaskState("My task", resource=resource)
    succeed_state = SucceedState("Success")
    pass_state = PassState("Just passing")
    fail_state = FailState("Failure", error="IFailed", cause="I failed!")
    task_state >> succeed_state
    pass_state >> fail_state
    task_state.add_catcher(["States.ALL"], next_state=pass_state)
    state_machine = StateMachine(start_state=task_state)

    def failure_mock_fn(event, context):
        assert False  # noqa: PT015

    output_path = tmp_path / "state_machine.gif"
    assert not output_path.exists()
    state_machine.simulate(
        resource_to_mock_fn={resource: failure_mock_fn},
        show_visualization=True,
        visualization_output_path=output_path,
    )
    assert output_path.exists()


def test_visualization_choice_state(tmp_path):
    public_state = PassState("Public")
    value_in_twenties_state = PassState("ValueInTwenties")
    after_value_in_twenties_state = SucceedState("Success!")
    start_audit_state = PassState("StartAudit")
    record_event_state = PassState("RecordEvent")

    value_in_twenties_state >> after_value_in_twenties_state

    choice_state = ChoiceState(
        "DispatchEvent",
        choices=[
            NotChoice(
                variable="$.type",
                string_equals="Private",
                next_state=public_state,
            ),
            AndChoice(
                [
                    ChoiceRule(variable="$.value", is_present=True),
                    ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
                    ChoiceRule(variable="$.value", numeric_less_than=30),
                ],
                next_state=value_in_twenties_state,
            ),
            VariableChoice(
                variable="$.rating",
                numeric_greater_than_path="$.auditThreshold",
                next_state=start_audit_state,
            ),
        ],
    )

    state_machine = StateMachine(start_state=choice_state)

    output_path = tmp_path / "state_machine.gif"
    assert not output_path.exists()
    state_machine.simulate(
        {"type": "Private", "value": 22},
        show_visualization=True,
        visualization_output_path=output_path,
    )
    assert output_path.exists()

    # Test with a default
    output_path.unlink()
    choice_state = ChoiceState(
        "DispatchEvent",
        choices=[
            NotChoice(
                variable="$.type",
                string_equals="Private",
                next_state=public_state,
            ),
            AndChoice(
                [
                    ChoiceRule(variable="$.value", is_present=True),
                    ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
                    ChoiceRule(variable="$.value", numeric_less_than=30),
                ],
                next_state=value_in_twenties_state,
            ),
            VariableChoice(
                variable="$.rating",
                numeric_greater_than_path="$.auditThreshold",
                next_state=start_audit_state,
            ),
        ],
        default=record_event_state,
    )
    state_machine = StateMachine(start_state=choice_state)
    assert not output_path.exists()
    state_machine.simulate(
        {"type": "Private", "value": 102, "auditThreshold": 150},
        show_visualization=True,
        visualization_output_path=output_path,
    )
    assert output_path.exists()
