from awsstepfuncs import PassState, StateMachine


def test_pass_state(capture_stdout):
    pass_state1 = PassState("Pass 1", comment="The starting state")
    pass_state2 = PassState("Pass 2")
    pass_state3 = PassState("Pass 3")

    pass_state1 >> pass_state2 >> pass_state3
    state_machine = StateMachine(start_state=pass_state1)

    assert [state.name for state in state_machine.start_state] == [
        "Pass 1",
        "Pass 2",
        "Pass 3",
    ]

    assert state_machine.compile() == {
        "StartAt": "Pass 1",
        "States": {
            "Pass 2": {"Type": "Pass", "Next": "Pass 3"},
            "Pass 1": {
                "Type": "Pass",
                "Comment": "The starting state",
                "Next": "Pass 2",
            },
            "Pass 3": {"Type": "Pass", "End": True},
        },
    }

    stdout = capture_stdout(lambda: state_machine.simulate())

    assert (
        stdout
        == """Starting simulation of state machine
Executing PassState('Pass 1')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {}
State output after applying output path of $: {}
State output: {}
Executing PassState('Pass 2')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {}
State output after applying output path of $: {}
State output: {}
Executing PassState('Pass 3')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {}
State output after applying output path of $: {}
State output: {}
Terminating simulation of state machine
"""
    )


def test_pass_state_result(capture_stdout):
    result = {"Hello": "world!"}
    pass_state = PassState("Passing", result=result)
    state_machine = StateMachine(start_state=pass_state)

    assert state_machine.compile() == {
        "StartAt": "Passing",
        "States": {
            "Passing": {"Type": "Pass", "End": True, "Result": {"Hello": "world!"}}
        },
    }

    stdout = capture_stdout(lambda: state_machine.simulate())

    assert (
        stdout
        == """Starting simulation of state machine
Executing PassState('Passing')
State input: {}
State input after applying input path of $: {}
Output from applying result path of $: {'Hello': 'world!'}
State output after applying output path of $: {'Hello': 'world!'}
State output: {'Hello': 'world!'}
Terminating simulation of state machine
"""
    )
