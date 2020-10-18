from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from awsstepfuncs.json_path import apply_json_path
from awsstepfuncs.state import (
    AbstractNextOrEndState,
    AbstractResultPathState,
    TaskState,
)

CompiledState = Dict[str, Union[str, bool, Dict[str, str], None]]


class StateMachine:
    """An AWS Step Functions state machine."""

    def __init__(
        self,
        *,
        start_state: AbstractNextOrEndState,
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
    def _has_unique_names(start_state: AbstractNextOrEndState) -> bool:
        """Check if all states have unique names.

        All state names must be unique in a state machine.

        Args:
            start_state: The starting state (which has references to all
                following states).

        Returns:
            Whether all states have unique names.
        """
        all_state_names = [state.name for state in start_state]
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

        state_output = None
        for state in self.start_state:
            state_output = self._simulate_state(state, state_input, resource_to_mock_fn)
            state_input = state_output

        return state_output

    def _simulate_state(
        self,
        state: AbstractNextOrEndState,
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
        state_input = apply_json_path(state.input_path, state_input)

        # Run the state to get the state output
        if isinstance(state, TaskState):
            state_output = state.run(
                state_input, mock_fn=resource_to_mock_fn[state.resource]
            )
        else:
            state_output = state.run(state_input)

        # Apply the ResultSelector to filter the state output
        # TODO: Add Map, Parallel here
        if isinstance(state, TaskState) and (result_selector := state.result_selector):
            state_output = self._apply_result_selector(state_output, result_selector)

        if isinstance(state, AbstractResultPathState):
            result_path_output = self._apply_result_path(
                state_input, state_output, state.result_path
            )
            return apply_json_path(state.output_path, result_path_output)
        else:
            return apply_json_path(state.output_path, state_output)

    @staticmethod
    def _apply_result_selector(
        state_output: Any, result_selector: Dict[str, str]
    ) -> Dict[str, Any]:
        """Apply the ResultSelector to select a portion of the state output.

        Args:
            state_output: The state output to filter.
            result_selector: The result selector which defines which fields to
                keep.

        Returns:
            The filtered state output.
        """
        new_state_output = {}
        for key, json_path in result_selector.items():
            key = key[:-2]  # Strip ".$"
            if extracted := apply_json_path(json_path, state_output):
                new_state_output[key] = extracted

        return new_state_output

    @staticmethod
    def _apply_result_path(
        state_input: Any, state_output: Any, result_path: Optional[str]
    ) -> Any:
        """Apply ResultPath to combine state input with state output.

        Args:
            state_input: The input state.
            state_output: The output state.
            result_path: A string that indicates whether to keep only the output
                state, only the input state, or the output state as a key of the
                input state.

        Returns:
            The state resulting from applying ResultPath.
        """
        if result_path == "$":
            # Just keep state output
            return state_output

        elif result_path is None:
            # Just keep state input, discard state_output
            return state_input

        elif match := re.fullmatch(r"\$\.([A-Za-z]+)", result_path):
            # Move the state output as a key in state input
            result_key = match.group(1)
            state_input[result_key] = state_output
            return state_input

        else:  # pragma: no cover
            assert False, "Should never happen"  # noqa: PT015
