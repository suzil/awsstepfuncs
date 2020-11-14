from __future__ import annotations

from typing import Optional, Type


class AWSStepFuncsError(Exception):
    """Base error for this package.

    This is useful for client code to know whether the error is expected or
    whether there is a bug in the library code.

    >>> from awsstepfuncs import *

    >>> try:
    ...     wait_state = WaitState("Wait!", seconds=-1)
    ... except AWSStepFuncsError:
    ...     print("Error in the client")
    ... except Exception:
    ...     print("Error in awsstepfuncs")
    Error in the client
    """


class AWSStepFuncsValueError(AWSStepFuncsError):
    """A bad value was specified.

    >>> from awsstepfuncs import *

    >>> wait_state = WaitState("Wait!", seconds=-1)
    Traceback (most recent call last):
        ...
    awsstepfuncs.errors.AWSStepFuncsValueError: seconds must be greater than zero
    """


class StateSimulationError(AWSStepFuncsError):
    """Raised when there is an error while simulating a state.

    Check this table for a list of state simulation errors: https://states-language.net/spec.html#appendix-a
    """

    # Generally a more specific error should be raised, but the
    # StateSimulationError may be raised if there is no other error that seems to
    # apply (potentially due to my lack of understanding).
    #
    # For example, the Wait State can have a reference path evaluated on the state
    # input that doesn't lead to a valid value. For this case, right now we just
    # raise the generic StateSimulationError. TODO: What error occurs when this
    # actually happens in AWS Step Functions?

    error_string = "States.ALL"

    @staticmethod
    def from_string(error_string: str) -> Optional[Type[StateSimulationError]]:
        """Convert a state simulation error string to an error class.

        >>> StateSimulationError.from_string("States.Timeout")
        <class 'awsstepfuncs.errors.StateTimeoutError'>

        If no value is returned, then the error cannot be simulated.

        >>> StateSimulationError.from_string("States.Permissions") is None
        True

        An `AWSStepFuncsValueError` will be raised if the error string is
        invalid.

        >>> StateSimulationError.from_string("Invalid error string")
        Traceback (most recent call last):
            ...
        awsstepfuncs.errors.AWSStepFuncsValueError: "Invalid error string" is not a valid error name

        Args:
            error_string: The error string, such as "States.ALL".

        Raises:
            AWSStepFuncsValueError: Raised when the error_string provided is
                invalid.

        Returns:
            The error class if the error can be simulated.
        """
        mapping = {
            error_class.error_string: error_class
            for error_class in {
                StateSimulationError,
                StateTimeoutError,
                TaskFailedError,
                NoChoiceMatchedError,
            }
        }
        compilation_only_errors = {
            "States.Permissions",
            "States.ResultPathMatchFailure",
            "States.ParameterPathFailure",
            "States.BranchFailed",
            "States.IntrinsicFailure",
        }
        if error_string not in set(mapping) | compilation_only_errors:
            raise AWSStepFuncsValueError(f'"{error_string}" is not a valid error name')
        return mapping.get(error_string)


class StateTimeoutError(StateSimulationError):
    """Raised when the state has timed out while simulating.

    A Task State either ran longer than the "TimeoutSeconds" value, or failed
    to heartbeat for a time longer than the "HeartbeatSeconds" value.
    """

    error_string = "States.Timeout"


class TaskFailedError(StateSimulationError):
    """Raised when the task has failed during the execution."""

    error_string = "States.TaskFailed"


class NoChoiceMatchedError(StateSimulationError):
    """Raised when a Choice State failed to pick a next state."""

    error_string = "States.NoChoiceMatched"
