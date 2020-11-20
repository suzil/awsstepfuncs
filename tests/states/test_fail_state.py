from awsstepfuncs import FailState, StateMachine


def test_fail_state(capture_stdout):
    fail_state = FailState("Failure", error="IFailed", cause="I failed!")
    state_machine = StateMachine(start_state=fail_state)
    state_machine.compile() == {
        "Type": "Fail",
        "Error": "IFailed",
        "Cause": "I failed!",
    }
    stdout = capture_stdout(lambda: state_machine.simulate())
    assert (
        stdout
        == """Starting simulation of state machine
Executing FailState('Failure', error='IFailed', cause='I failed!')
State input: {}
FailStateError encountered in state
Checking for catchers
State output: {}
Terminating simulation of state machine
"""
    )
