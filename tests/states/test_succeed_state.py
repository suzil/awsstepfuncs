from awsstepfuncs import StateMachine, SucceedState


def test_succeed_state(capture_stdout):
    succeed_state = SucceedState("Success!")
    state_machine = StateMachine(start_state=succeed_state)
    stdout = capture_stdout(lambda: state_machine.simulate({"Hello": "world!"}))
    assert (
        stdout
        == """Starting simulation of state machine
Executing SucceedState('Success!')
State input: {'Hello': 'world!'}
State input after applying input path of $: {'Hello': 'world!'}
State output after applying output path of $: {'Hello': 'world!'}
State output: {'Hello': 'world!'}
Terminating simulation of state machine
"""
    )
