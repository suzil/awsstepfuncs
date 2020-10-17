from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from awsstepfuncs.json_path import validate_json_path


class State(ABC):
    """An AWS Step Functions state."""

    state_type: Optional[StateType] = None

    def __init__(
        self,
        name: str,
        /,
        *,
        comment: Optional[str] = None,
        input_path: str = "$",
    ):
        """Initialize a state.

        Args:
            name: The name of the state.
            comment: A human-readable description of the state.
            input_path: Used to select a portion of the state input.

        Raises:
            ValueError: Raised when the input path is an invalid JSONPath.
        """
        self.name = name
        self.comment = comment
        self.next_state: Optional[State] = None

        try:
            validate_json_path(input_path)
        except ValueError:
            raise
        else:
            self.input_path = input_path

    def __init_subclass__(cls) -> None:
        """Validate subclasses.

        Args:
            cls: The subclass being validated.

        Raises:
            ValueError: When state type is not specified.
        """
        super().__init_subclass__()
        if not cls.state_type:  # pragma: no cover
            raise ValueError("Must specify state_type attribute")

    def __rshift__(self, other: State, /) -> State:
        """Overload >> operator when state execution order.

        Args:
            other: The other state besides self.

        Returns:
            The latest state (for right shift, the right state).
        """
        self.next_state = other
        return other

    def __iter__(self) -> State:
        """Iterate through the states."""
        self._current: Optional[State] = self
        return self._current

    def __next__(self) -> State:
        """Get the next state."""
        current = self._current
        if not current:
            raise StopIteration

        self._current = current.next_state
        return current

    @abstractmethod
    def run(self, state_input: dict) -> dict:
        """Execute the state.

        Args:
            state_input: The input state data.

        Raises:
            NotImplementedError: Raised if not implemented by subclasses.
        """
        raise NotImplementedError


class StateType(Enum):
    """State types in Amazon States Language."""

    PASS = "Pass"
    TASK = "Task"
