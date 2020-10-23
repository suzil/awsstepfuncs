import pytest

from awsstepfuncs import PassState, StateMachine
from awsstepfuncs.state import FailState, TaskState


def test_one_state(compile_state_machine):
    pass_state = PassState("My Pass", comment="The only state")
    state_machine = StateMachine(
        start_state=pass_state, comment="My state machine", version="1.1"
    )
    compiled = compile_state_machine(state_machine)
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


def test_duplicate_names():
    duplicate_name = "My Pass"
    pass_state1 = PassState(duplicate_name)
    pass_state2 = PassState(duplicate_name)
    pass_state1 >> pass_state2
    with pytest.raises(
        ValueError,
        match="Duplicate names detected in state machine. Names must be unique",
    ):
        StateMachine(start_state=pass_state1)


def test_duplicate_names_catcher():
    duplicate_name = "Duplicated"
    task_state = TaskState(duplicate_name, resource="123")
    transition_state = FailState(duplicate_name, error="MyError", cause="Negligence")
    task_state.add_catcher(["Something"], next_state=transition_state)
    with pytest.raises(
        ValueError,
        match="Duplicate names detected in state machine. Names must be unique",
    ):
        StateMachine(start_state=task_state)
