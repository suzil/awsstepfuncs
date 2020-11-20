import pytest

from awsstepfuncs import FailState, PassState
from awsstepfuncs.errors import AWSStepFuncsValueError


def test_state_name_too_long():
    with pytest.raises(
        AWSStepFuncsValueError, match="State name cannot exceed 128 characters"
    ):
        PassState("a" * 129)


def test_terminal_state():
    fail_state = FailState("Fail", error="JustBecause", cause="Because I feel like it")
    pass_state = PassState("Pass")
    with pytest.raises(
        AWSStepFuncsValueError, match="FailState cannot have a next state"
    ):
        fail_state >> pass_state
