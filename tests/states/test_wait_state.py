from datetime import datetime, timedelta

import pytest

from awsstepfuncs import StateMachine, WaitState
from awsstepfuncs.errors import AWSStepFuncsValueError


def test_wait_state(capture_stdout):
    wait_state = WaitState("Wait!", seconds=1)
    state_machine = StateMachine(start_state=wait_state)
    stdout = capture_stdout(lambda: state_machine.simulate())
    assert (
        stdout
        == """Starting simulation of state machine
Executing WaitState('Wait!', seconds=1)
State input: {}
State input after applying input path of $: {}
Waiting 1 seconds
State output after applying output path of $: {}
State output: {}
Terminating simulation of state machine
"""
    )


def test_negative_seconds():
    with pytest.raises(
        AWSStepFuncsValueError, match="seconds must be greater than zero"
    ):
        WaitState("Wait!", seconds=-1)


def test_future_timestamp(capture_stdout):
    timestamp = datetime.now() + timedelta(seconds=2)
    wait_state = WaitState("Wait", timestamp=timestamp)
    state_machine = StateMachine(start_state=wait_state)
    state_input = {"foo": "bar"}

    stdout = capture_stdout(lambda: state_machine.simulate(state_input))
    assert f"Waiting until {timestamp.isoformat()}" in stdout


def test_past_timestamp(capture_stdout):
    wait_state = WaitState("Wait!", timestamp=datetime(2020, 1, 1))
    state_machine = StateMachine(start_state=wait_state)
    stdout = capture_stdout(lambda: state_machine.simulate())
    assert (
        stdout
        == """Starting simulation of state machine
Executing WaitState('Wait!', timestamp='2020-01-01T00:00:00')
State input: {}
State input after applying input path of $: {}
State output after applying output path of $: {}
State output: {}
Terminating simulation of state machine
"""
    )


def test_seconds_path(capture_stdout):
    wait_state = WaitState("Wait!", seconds_path="$.numSeconds")
    state_machine = StateMachine(start_state=wait_state)
    stdout = capture_stdout(lambda: state_machine.simulate({"numSeconds": 1}))
    assert (
        stdout
        == """Starting simulation of state machine
Executing WaitState('Wait!', seconds_path='$.numSeconds')
State input: {'numSeconds': 1}
State input after applying input path of $: {'numSeconds': 1}
Waiting 1 seconds
State output after applying output path of $: {'numSeconds': 1}
State output: {'numSeconds': 1}
Terminating simulation of state machine
"""
    )


def test_invalid_seconds_path(capture_stdout):
    wait_state = WaitState("Wait!", seconds_path="$.numSeconds")
    state_machine = StateMachine(start_state=wait_state)
    stdout = capture_stdout(lambda: state_machine.simulate({"numSeconds": "hello"}))
    assert (
        stdout
        == """Starting simulation of state machine
Executing WaitState('Wait!', seconds_path='$.numSeconds')
State input: {'numSeconds': 'hello'}
State input after applying input path of $: {'numSeconds': 'hello'}
StateSimulationError encountered in state
Checking for catchers
State output: {}
Terminating simulation of state machine
"""
    )


def test_timestamp_path(capture_stdout):
    wait_state = WaitState("Wait!", timestamp_path="$.meta.timeToWait")
    state_machine = StateMachine(start_state=wait_state)
    stdout = capture_stdout(
        lambda: state_machine.simulate({"meta": {"timeToWait": "2020-01-01T00:00:00"}})
    )
    assert (
        stdout
        == """Starting simulation of state machine
Executing WaitState('Wait!', timestamp_path='$.meta.timeToWait')
State input: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
State input after applying input path of $: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
Waiting until 2020-01-01T00:00:00
State output after applying output path of $: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
State output: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
Terminating simulation of state machine
"""
    )


def test_too_many_parameters():
    with pytest.raises(
        AWSStepFuncsValueError,
        match="Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path",
    ):
        WaitState("Wait", seconds=5, timestamp=datetime.now())


def test_no_parameters_set():
    with pytest.raises(
        AWSStepFuncsValueError,
        match="Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path",
    ):
        WaitState("Wait")
