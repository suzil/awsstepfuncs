from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Union

from awsstepfuncs.json_path import apply_json_path
from awsstepfuncs.state import State
from awsstepfuncs.task_state import TaskState


class StateMachine:
    """An AWS Step Functions state machine."""

    def __init__(self, *, start_state: State):
        """Initialize a state machine.

        A state machine will contain a reference to the start state. All states
        in the state machine must have a unique name.

        Args:
            start_state: The starting state.

        Raises:
            ValueError: Raised when there are duplicate state names.
        """
        if not self._has_unique_names(start_state):
            raise ValueError(
                "Duplicate names detected in state machine. Names must be unique"
            )

        self.start_state = start_state

    @staticmethod
    def _has_unique_names(start_state: State) -> bool:
        all_state_names = [state.name for state in start_state]
        return len(all_state_names) == len(set(all_state_names))

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
            "Type": state.state_type.value,  # type: ignore
        }
        if comment := state.comment:
            compiled["Comment"] = comment

        if isinstance(state, TaskState):
            compiled["Resource"] = state.resource_uri

        if input_path := state.input_path:
            compiled["InputPath"] = input_path

        if next_state := state.next_state:
            compiled["Next"] = next_state.name
        else:
            compiled["End"] = True

        return compiled

    def simulate(
        self,
        *,
        state_input: dict = None,
        resource_to_mock_fn: Dict[str, Callable] = None,
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

        for state in self.start_state:
            state_output = self._simulate_state(state, state_input, resource_to_mock_fn)
            state_input = state_output

        return state_output

    @staticmethod
    def _simulate_state(
        state: State,
        state_input: Any,
        resource_to_mock_fn: Dict[str, Callable],
    ) -> Any:
        """Simulate a single state.

        Args:
            state: The state to simulate.
            state_input: Data to pass to the state.
            resource_to_mock_fn: A dictionary mapping Resource URI to a mock
                function to use in the simulation.

        Returns:
            The output state.
        """
        print(f"Running {state.name}")  # noqa: T001

        # Use input_path to select a subset of the input state to process if
        # defined
        if input_path := state.input_path:
            state_input = apply_json_path(input_path, state_input)

        if isinstance(state, TaskState):
            return state.run(
                state_input, mock_fn=resource_to_mock_fn[state.resource_uri]
            )
        else:
            return state.run(state_input)
