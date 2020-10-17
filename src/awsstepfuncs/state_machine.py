from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Union

from jsonpath_rw import parse as parse_jsonpath

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
    ) -> dict:
        """Simulate the state machine by executing all of the states.

        Args:
            state_input: A dictionary representing data to pass to the first
                state.
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
            print(f"Running {state.name}")  # noqa: T001
            if isinstance(state, TaskState):
                state_output = state.run(
                    state_input, mock_fn=resource_to_mock_fn[state.resource_uri]
                )
            else:
                state_output = state.run(state_input)

            state_input = state_output

        return state_output

    @staticmethod
    def _apply_json_path(json_path: str, data: dict) -> Any:
        """Parse then apply a JSONPath on some data.

        Args:
            json_path: The JSONPath to parse and apply.
            data: The data to use the JSONPath expression on.

        Raises:
            ValueError: Raised when JSONPath is an empty string or does not
                begin with a "$".
            ValueError: Raised when the JSONPath has an unsupported operator (an
                operator that AWS Step Functions does not support).
            ValueError: Raised when the JSONPath does not find any match. There
                should always be some match found.

        Returns:
            The queried data.
        """
        if not json_path or json_path[0] != "$":
            raise ValueError('JSONPath must begin with "$"')

        unsupported_operators = {"@", "..", ",", ":", "?", "*"}
        for operator in unsupported_operators:
            if operator in json_path:
                raise ValueError(f'Unsupported JSONPath operator: "{operator}"')

        parsed_json_path = parse_jsonpath(json_path)
        if matches := [match.value for match in parsed_json_path.find(data)]:
            return matches[0]
        else:
            raise ValueError(f'JSONPath "{json_path}" did not find a match')
