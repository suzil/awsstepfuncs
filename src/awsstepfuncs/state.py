from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional


class State(ABC):
    """An AWS Step Functions state."""

    state_type: Optional[StateType] = None

    def __init__(self, name: str, /, *, description: Optional[str] = None):
        """Initialize a state.

        Args:
            name: The name of the state.
            description: A description of the state.
        """
        self.name = name
        self.description = description
        self.next_state: Optional[State] = None

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
    def run(self) -> None:
        """Execute the state."""
        raise NotImplementedError


class StateType(Enum):
    """State types in Amazon States Language."""

    PASS = "Pass"
    LAMBDA = "Task"
