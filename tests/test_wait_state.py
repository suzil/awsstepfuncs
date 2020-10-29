import contextlib
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from io import StringIO

from awsstepfuncs import StateMachine, WaitState


def test_wait_state_later_timestamp():
    timestamp = datetime.now() + timedelta(seconds=2)
    wait_state = WaitState("Wait", timestamp=timestamp)
    state_machine = StateMachine(start_state=wait_state)
    state_input = {"foo": "bar"}

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_machine.simulate(state_input=state_input)
        stdout = fp.getvalue()

    assert f"Waiting until {timestamp.isoformat()}" in stdout
