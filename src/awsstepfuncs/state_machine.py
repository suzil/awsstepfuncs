from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

from awsstepfuncs.state import AbstractRetryCatchState, AbstractState
from awsstepfuncs.types import ResourceToMockFn

CompiledState = Dict[str, Union[str, bool, Dict[str, str], None]]


class StateMachine:
    """An AWS Step Functions state machine."""

    def __init__(
        self,
        *,
        start_state: AbstractState,
        comment: Optional[str] = None,
        version: Optional[str] = None,
    ):
        """Initialize a state machine.

        A state machine will contain a reference to the start state. All states
        in the state machine must have a unique name.

        Args:
            start_state: The starting state.
            comment: A human-readable description of the state machine.
            version: The verison of the state machine. If omitted, defaults to
                "1.0".

        Raises:
            ValueError: Raised when there are duplicate state names.
        """
        if not self._has_unique_names(start_state):
            raise ValueError(
                "Duplicate names detected in state machine. Names must be unique"
            )
        self.start_state = start_state
        self.comment = comment
        self.version = version

    @staticmethod
    def _has_unique_names(start_state: AbstractState) -> bool:
        """Check if all states have unique names.

        All state names must be unique in a state machine.

        Args:
            start_state: The starting state (which has references to all
                following states).

        Returns:
            Whether all states have unique names.
        """
        all_states = set()
        for state in start_state:
            all_states.add(state)
            if isinstance(state, AbstractRetryCatchState):
                for catcher in state.catchers:
                    all_states.add(catcher.next_state)

        all_state_names = [state.name for state in all_states]
        return len(all_state_names) == len(set(all_state_names))

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile a state machine to Amazon States Language.

        Returns:
            A dictionary of the compiled state in Amazon States Language.
        """
        compiled = {
            "StartAt": self.start_state.name,
            "States": {state.name: state.compile() for state in self.start_state},
        }

        if comment := self.comment:
            compiled["Comment"] = comment

        if version := self.version:
            compiled["Version"] = version

        return compiled

    def to_json(self, filename: Union[str, Path]) -> None:
        """Compile to Amazon States Language and then output to JSON.

        Args:
            filename: The name of the file to write the JSON to.
        """
        filename = Path(filename)
        with filename.open("w") as fp:
            json.dump(self.compile(), fp)

    def simulate(
        self,
        *,
        state_input: dict = None,
        resource_to_mock_fn: ResourceToMockFn = None,
    ) -> Any:
        """Simulate the state machine by executing all of the states.

        Args:
            state_input: Data to pass to the first state.
            resource_to_mock_fn: A dictionary mapping Resource URI to a mock
                function to use in the simulation.

        Returns:
            The final output state from simulating the state machine.
        """
        if state_input is None:
            state_input = {}

        if resource_to_mock_fn is None:
            resource_to_mock_fn = {}

        state_output = None
        for state in self.start_state:
            state_output = state.simulate(state_input, resource_to_mock_fn)
            state_input = state_output

        return state_output
