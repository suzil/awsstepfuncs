import contextlib
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from io import StringIO

import pytest

from awsstepfuncs import StateMachine, WaitState


def test_wait_state_seconds(compile_state_machine):
    n_seconds = 5
    wait_state = WaitState("Wait", seconds=n_seconds)

    # Define the state machine
    state_machine = StateMachine(start_state=wait_state)

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": wait_state.name,
        "States": {
            wait_state.name: {
                "Type": "Wait",
                "Seconds": n_seconds,
                "End": True,
            },
        },
    }

    state_input = {"foo": "bar"}

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate(state_input=state_input)
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert state_output == state_input
    assert stdout == [
        "Starting simulation of state machine",
        "Running Wait",
        f"State input: {state_input}",
        f'State input after applying input path of "$": {state_input}',
        f"Waiting {n_seconds} seconds",
        f'State output after applying output path of "$": {state_input}',
        f"State output: {state_input}",
        "Terminating simulation of state machine",
    ]


def test_wait_state_earlier_timestamp(compile_state_machine):
    timestamp = datetime.now() - timedelta(days=5)
    wait_state = WaitState("Wait", timestamp=timestamp)

    # Define the state machine
    state_machine = StateMachine(start_state=wait_state)

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": wait_state.name,
        "States": {
            wait_state.name: {
                "Type": "Wait",
                "Timestamp": timestamp.isoformat(),
                "End": True,
            },
        },
    }

    state_input = {"foo": "bar"}

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate(state_input=state_input)
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert state_output == state_input
    assert stdout == [
        "Starting simulation of state machine",
        "Running Wait",
        f"State input: {state_input}",
        f'State input after applying input path of "$": {state_input}',
        f'State output after applying output path of "$": {state_input}',
        f"State output: {state_input}",
        "Terminating simulation of state machine",
    ]


def test_wait_state_later_timestamp(compile_state_machine):
    timestamp = datetime.now() + timedelta(seconds=2)
    wait_state = WaitState("Wait", timestamp=timestamp)

    # Define the state machine
    state_machine = StateMachine(start_state=wait_state)

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": wait_state.name,
        "States": {
            wait_state.name: {
                "Type": "Wait",
                "Timestamp": timestamp.isoformat(),
                "End": True,
            },
        },
    }

    state_input = {"foo": "bar"}

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate(state_input=state_input)
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert state_output == state_input
    assert stdout == [
        "Starting simulation of state machine",
        "Running Wait",
        f"State input: {state_input}",
        f'State input after applying input path of "$": {state_input}',
        f"Waiting until {timestamp.isoformat()}",
        f'State output after applying output path of "$": {state_input}',
        f"State output: {state_input}",
        "Terminating simulation of state machine",
    ]


def test_both_seconds_and_timestamp():
    with pytest.raises(
        ValueError,
        match="Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path",
    ):
        WaitState("Wait", seconds=5, timestamp=datetime.now())


def test_neither_seconds_and_timestamp():
    with pytest.raises(
        ValueError,
        match="Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path",
    ):
        WaitState("Wait", seconds=None, timestamp=None)
