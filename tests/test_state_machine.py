import json

import pytest

from awsstepfuncs import (
    AWSStepFuncsValueError,
    FailState,
    PassState,
    StateMachine,
    TaskState,
)


def test_duplicate_names():
    duplicate_name = "My Pass"
    pass_state1 = PassState(duplicate_name)
    pass_state2 = PassState(duplicate_name)
    pass_state1 >> pass_state2
    with pytest.raises(
        AWSStepFuncsValueError,
        match="Duplicate names detected in state machine. Names must be unique",
    ):
        StateMachine(start_state=pass_state1)


def test_duplicate_names_catcher():
    duplicate_name = "Duplicated"
    task_state = TaskState(duplicate_name, resource="123")
    transition_state = FailState(duplicate_name, error="MyError", cause="Negligence")
    task_state.add_catcher(["Something"], next_state=transition_state)
    with pytest.raises(
        AWSStepFuncsValueError,
        match="Duplicate names detected in state machine. Names must be unique",
    ):
        StateMachine(start_state=task_state)


def test_to_json(tmp_path):
    pass_state = PassState("My Pass", comment="The only state")
    state_machine = StateMachine(
        start_state=pass_state, comment="My state machine", version="1.1"
    )

    compiled_path = tmp_path / "state_machine.json"
    state_machine.to_json(compiled_path)
    with compiled_path.open() as fp:
        compiled = json.load(fp)

    assert compiled == {
        "StartAt": pass_state.name,
        "Comment": state_machine.comment,
        "Version": state_machine.version,
        "States": {
            pass_state.name: {
                "Comment": pass_state.comment,
                "Type": "Pass",
                "End": True,
            },
        },
    }
