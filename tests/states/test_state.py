import pytest

from awsstepfuncs import PassState
from awsstepfuncs.errors import AWSStepFuncsValueError


def test_state_name_too_long():
    with pytest.raises(
        AWSStepFuncsValueError, match="State name cannot exceed 128 characters"
    ):
        PassState("a" * 129)
