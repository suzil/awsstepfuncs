from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional, Union


class StateMachine:
    """An AWS Step Functions state machine."""

    def __init__(self, *, start_state: State):
        """Initialize a state machine.

        A state machine will contain a reference to the start state, the root node in
        our state machine DAG.

        Args:
            start_state: The starting state.
        """
        self.start_state = start_state

    def compile(self, output_path: Union[str, Path]) -> None:  # noqa: A003
        """Compile a state machine to Amazon States Language.

        Args:
            output_path: The path to save the compiled JSON.
        """
        output_path = Path(output_path)
        compiled = {
            "StartAt": self.start_state.name,
            "States": {
                state.name: self._compile_state(state) for state in self.start_state
            },
        }
        with output_path.open("w") as fp:
            json.dump(compiled, fp)

    @staticmethod
    def _compile_state(state: State, /) -> Dict[str, Union[str, bool]]:
        """Compile a state to Amazon States Language.

        Args:
            state: The state to compile.

        Returns:
            The compiled representation of the state.
        """
        compiled: Dict[str, Union[str, bool]] = {
            "Type": "Pass",  # TODO: Make it generic with an enum, init_subclasses
        }
        if description := state.description:
            compiled["Comment"] = description

        if next_state := state.next_state:
            compiled["Next"] = next_state.name
        else:
            compiled["End"] = True

        return compiled


class State:
    """An AWS Step Functions state."""

    def __init__(self, name: str, /, *, description: Optional[str] = None):
        """Initialize a state.

        Args:
            name: The name of the state.
            description: A description of the state.
        """
        self.name = name
        self.description = description
        self.next_state: Optional[State] = None

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
