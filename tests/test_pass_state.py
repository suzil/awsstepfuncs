import contextlib
from contextlib import redirect_stdout
from io import StringIO

import pytest

from awsstepfuncs import PassState, StateMachine
from awsstepfuncs.state import MAX_STATE_NAME_LENGTH


def test_pass_state(compile_state_machine):
    pass_state1 = PassState("Pass 1", comment="The starting state")
    pass_state2 = PassState("Pass 2")
    pass_state3 = PassState("Pass 3")

    # Define the state machine
    pass_state1 >> pass_state2 >> pass_state3
    state_machine = StateMachine(start_state=pass_state1)

    # Make sure that the DAG is correctly specified
    assert [state.name for state in state_machine.start_state] == [
        pass_state1.name,
        pass_state2.name,
        pass_state3.name,
    ]

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": pass_state1.name,
        "States": {
            pass_state1.name: {
                "Comment": pass_state1.comment,
                "Type": "Pass",
                "Next": pass_state2.name,
            },
            pass_state2.name: {
                "Type": "Pass",
                "Next": pass_state3.name,
            },
            pass_state3.name: {
                "Type": "Pass",
                "End": True,
            },
        },
    }

    # Simulate the state machine
    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate()
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert state_output == {}
    assert stdout == [
        "Starting simulation of state machine",
        "Running PassState('Pass 1')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        'Output from applying result path of "$": {}',
        'State output after applying output path of "$": {}',
        "State output: {}",
        "Running PassState('Pass 2')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        'Output from applying result path of "$": {}',
        'State output after applying output path of "$": {}',
        "State output: {}",
        "Running PassState('Pass 3')",
        "State input: {}",
        'State input after applying input path of "$": {}',
        'Output from applying result path of "$": {}',
        'State output after applying output path of "$": {}',
        "State output: {}",
        "Terminating simulation of state machine",
    ]


def test_state_name_too_long():
    with pytest.raises(
        ValueError, match=r'State name "[a]+" must be less than 128 characters'
    ):
        PassState("a" * (MAX_STATE_NAME_LENGTH + 1))
