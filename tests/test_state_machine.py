import re

import pytest

from awsstepfuncs import PassState, StateMachine


@pytest.fixture(scope="session")
def sample_data():
    return {
        "foo": 123,
        "bar": ["a", "b", "c"],
        "car": {
            "cdr": True,
        },
    }


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


@pytest.mark.parametrize(
    ("json_path", "match"),
    [("$.foo", 123), ("$.bar", ["a", "b", "c"]), ("$.car.cdr", True)],
)
def test_apply_json_path(json_path, match, sample_data):
    assert StateMachine._apply_json_path(json_path, sample_data) == match


def test_apply_json_path_unsupported_operator(sample_data):
    with pytest.raises(ValueError, match='Unsupported JSONPath operator: "*"'):
        StateMachine._apply_json_path("$foo[*].baz", sample_data)


def test_apply_json_path_must_begin_with_dollar(sample_data):
    with pytest.raises(ValueError, match=re.escape('JSONPath must begin with "$"')):
        StateMachine._apply_json_path("foo[*].baz", sample_data)


def test_apply_json_path_no_match(sample_data):
    with pytest.raises(
        ValueError, match=re.escape('JSONPath "$.notfound" did not find a match')
    ):
        StateMachine._apply_json_path("$.notfound", sample_data)
