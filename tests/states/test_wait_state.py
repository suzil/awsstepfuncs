from datetime import datetime, timedelta

from awsstepfuncs import StateMachine, WaitState


def test_wait_state_later_timestamp(capture_stdout):
    timestamp = datetime.now() + timedelta(seconds=2)
    wait_state = WaitState("Wait", timestamp=timestamp)
    state_machine = StateMachine(start_state=wait_state)
    state_input = {"foo": "bar"}

    stdout = capture_stdout(lambda: state_machine.simulate(state_input))
    assert f"Waiting until {timestamp.isoformat()}" in stdout
