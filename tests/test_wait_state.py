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
            state_output = state_machine.simulate(state_input=state_input)
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert state_output == state_input
    assert stdout == [
        "Starting simulation of state machine",
        "Running WaitState(name='Wait')",
        f"State input: {state_input}",
        f'State input after applying input path of "$": {state_input}',
        f"Waiting until {timestamp.isoformat()}",
        f'State output after applying output path of "$": {state_input}',
        f"State output: {state_input}",
        "Terminating simulation of state machine",
    ]
