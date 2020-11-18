from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from awsstepfuncs.errors import AWSStepFuncsValueError, StateSimulationError

if TYPE_CHECKING:  # pragma: no cover
    from awsstepfuncs.abstract_state import AbstractState


class AbstractErrorHandler(ABC):
    """Error handlers compose of Retriers and Catchers."""

    def __init__(self, error_equals: List[str]):
        """Initialize child classes with error_equals handled.

        Args:
            error_equals: A list of error names.
        """
        self.error_equals: List[Union[str, Type[StateSimulationError]]] = [
            error_class
            if (error_class := StateSimulationError.from_string(error_string))
            else error_string
            for error_string in error_equals
        ]

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the error handler with error_equals handled.

        Returns:
            A compilation with error_equals compiled.
        """
        return {
            "ErrorEquals": [
                error if isinstance(error, str) else error.error_string
                for error in self.error_equals
            ]
        }


class Retrier(AbstractErrorHandler):
    """Used to retry a failed state given the error names."""

    def __init__(
        self,
        error_equals: List[str],
        interval_seconds: Optional[int] = None,
        backoff_rate: Optional[float] = None,
        max_attempts: Optional[int] = None,
    ) -> None:
        """Initialize a Retrier.

        Args:
            error_equals: A list of error names.
            interval_seconds: The number of seconds before the first retry
                attempt. Defaults to 1 if not specified.
            backoff_rate: A number which is the multiplier that increases the
                retry interval on each attempt. Defaults to 2.0 if not specified.
            max_attempts: The maximum number of retry to attempt. Defaults to 3
                if not specified. A value of zero means that the error should never
                be retried.

        Raises:
            AWSStepFuncsValueError: Raised when interval_seconds is negative.
            AWSStepFuncsValueError: Raised when backoff_rate is less than 1.0.
            AWSStepFuncsValueError: Raised when max_attempts is negative.
        """
        if interval_seconds and interval_seconds <= 0:  # pragma: no cover
            raise AWSStepFuncsValueError("interval_seconds must be a positive integer")
        if backoff_rate and backoff_rate < 1:  # pragma: no cover
            raise AWSStepFuncsValueError(
                "backoff_rate must be greater than or equal to 1.0"
            )
        if max_attempts is not None and max_attempts < 0:  # pragma: no cover
            raise AWSStepFuncsValueError(
                "max_attempts must be zero or a positive integer"
            )

        super().__init__(error_equals)
        self.interval_seconds = interval_seconds
        self.backoff_rate = backoff_rate
        self.max_attempts = max_attempts

    def compile(self) -> Dict[str, Union[List[str], int, float]]:  # noqa: A003
        """Compile the Retrier to Amazon States Language.

        Returns:
            A Retrier in Amazon States Language.
        """
        compiled: Dict[str, Union[List[str], int, float]] = super().compile()
        if interval_seconds := self.interval_seconds:  # pragma: no cover
            compiled["IntervalSeconds"] = interval_seconds
        if backoff_rate := self.backoff_rate:  # pragma: no cover
            compiled["BackoffRate"] = backoff_rate
        if (max_attempts := self.max_attempts) is not None:  # pragma: no cover
            compiled["MaxAttempts"] = max_attempts
        return compiled


class Catcher(AbstractErrorHandler):
    """Used to go from an errored state to another state."""

    def __init__(self, error_equals: List[str], next_state: AbstractState):
        """Initialize a Catcher.

        Args:
            error_equals: A list of error names.
            next_state: The state to transition to if the Catcher is matched.
        """
        super().__init__(error_equals)
        self.next_state = next_state

    def compile(self) -> Dict[str, Union[List[str], str]]:  # noqa: A003
        """Compile the Catcher to Amazon States Language.

        Returns:
            A Catcher in Amazon States Language.
        """
        compiled: Dict[str, Union[List[str], str]] = super().compile()
        compiled["Next"] = self.next_state.name
        return compiled
