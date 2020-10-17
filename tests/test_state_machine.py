import pytest

from awsstepfuncs import PassState, StateMachine


def test_one_state(compile_state_machine):
    pass_state = PassState("My Pass", comment="The only state")
    state_machine = StateMachine(start_state=pass_state)
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": pass_state.name,
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
