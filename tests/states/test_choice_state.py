from awsstepfuncs import (
    AndChoice,
    ChoiceRule,
    ChoiceState,
    NotChoice,
    PassState,
    StateMachine,
    VariableChoice,
)


def test_choice_state(capture_stdout):
    # Define some states that can be conditionally transitioned to by the
    # Choice State

    public_state = PassState("Public")
    value_in_twenties_state = PassState("ValueInTwenties")
    start_audit_state = PassState("StartAudit")
    record_event_state = PassState("RecordEvent")

    # Now we can define a Choice State with branching logic based on
    # Choice Rules

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
    stdout = capture_stdout(
        lambda: state_machine.simulate({"type": "Private", "value": 22})
    )
    assert (
        stdout
        == """Starting simulation of state machine
Executing ChoiceState('DispatchEvent')
State input: {'type': 'Private', 'value': 22}
State input after applying input path of $: {'type': 'Private', 'value': 22}
State output after applying output path of $: {'type': 'Private', 'value': 22}
State output: {'type': 'Private', 'value': 22}
Executing PassState('ValueInTwenties')
State input: {'type': 'Private', 'value': 22}
State input after applying input path of $: {'type': 'Private', 'value': 22}
Output from applying result path of $: {'type': 'Private', 'value': 22}
State output after applying output path of $: {'type': 'Private', 'value': 22}
State output: {'type': 'Private', 'value': 22}
Terminating simulation of state machine
"""
    )
    # If no choice evaluates to true, then the default will be chosen

    stdout = capture_stdout(
        lambda: state_machine.simulate(
            {
                "type": "Private",
                "value": 102,
                "auditThreshold": 150,
            }
        )
    )
    assert (
        stdout
        == """Starting simulation of state machine
Executing ChoiceState('DispatchEvent')
State input: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
State input after applying input path of $: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
No choice evaluated to true
Choosing next state by the default set
State output after applying output path of $: {}
State output: {}
Executing PassState('RecordEvent')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {}
State output after applying output path of $: {}
State output: {}
Terminating simulation of state machine
"""
    )

    # If no choice evaluates to true and no default is set, then there will be an
    # error
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
    stdout = capture_stdout(
        lambda: state_machine.simulate(
            {
                "type": "Private",
                "value": 102,
                "auditThreshold": 150,
            }
        )
    )
    assert (
        stdout
        == """Starting simulation of state machine
Executing ChoiceState('DispatchEvent')
State input: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
State input after applying input path of $: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
No choice evaluated to true
NoChoiceMatchedError encountered in state
Checking for catchers
State output: {}
Terminating simulation of state machine
"""
    )
