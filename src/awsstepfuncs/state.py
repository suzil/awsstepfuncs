from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from awsstepfuncs.json_path import validate_json_path

MAX_STATE_NAME_LENGTH = 128


class AbstractState(ABC):
    """An AWS Step Functions state."""

    state_type: Optional[str] = None

    def __init__(
        self,
        name: str,
        /,
        *,
        comment: Optional[str] = None,
        input_path: str = "$",
        output_path: str = "$",
        result_path: Optional[
            str
        ] = "$",  # TODO: Only applies to Pass, Task, Parallel (Mixin?)
    ):
        """Initialize a state.

        Args:
            name: The name of the state. Must be unique within the state machine
                and its length cannot exceed 128 characters.
            comment: A human-readable description of the state.
            input_path: Used to select a portion of the state input. Default is
                $ (pass everything).
            output_path: Used to select a portion of the state output. Default
                is $ (pass everything).
            result_path: Specifies where (in the input) to place the "output" of
                the virtual task specified in Result. The input is further filtered
                as specified by the OutputPath field (if present) before being used
                as the state's output. Default is $ (pass only the output state).

        Raises:
            ValueError: Raised when the state name exceeds 128 characters.
            ValueError: Raised when an invalid JSONPath is specified.
        """
        if len(name) > MAX_STATE_NAME_LENGTH:
            raise ValueError(
                f'State name "{name}" must be less than {MAX_STATE_NAME_LENGTH} characters'
            )

        self.name = name
        self.comment = comment
        self.next_state: Optional[AbstractState] = None

        all_json_paths = [input_path, output_path]
        if result_path:
            all_json_paths.append(result_path)

        for json_path in all_json_paths:
            try:
                validate_json_path(json_path)
            except ValueError:
                raise

        self.input_path = input_path
        self.output_path = output_path
        self.result_path = result_path

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

    def __rshift__(self, other: AbstractState, /) -> AbstractState:
        """Overload >> operator when state execution order.

        Args:
            other: The other state besides self.

        Returns:
            The latest state (for right shift, the right state).
        """
        self.next_state = other
        return other

    def __iter__(self) -> AbstractState:
        """Iterate through the states."""
        self._current: Optional[AbstractState] = self
        return self._current

    def __next__(self) -> AbstractState:
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
