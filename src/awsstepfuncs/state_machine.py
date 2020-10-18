from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from awsstepfuncs.json_path import apply_json_path
from awsstepfuncs.pass_state import PassState
from awsstepfuncs.state import State
from awsstepfuncs.task_state import TaskState

CompiledState = Dict[str, Union[str, bool, Dict[str, str], None]]


class StateMachine:
    """An AWS Step Functions state machine."""

    def __init__(self, *, start_state: State, comment: Optional[str] = None):
        """Initialize a state machine.

        A state machine will contain a reference to the start state. All states
        in the state machine must have a unique name.

        Args:
            start_state: The starting state.
            comment: A human-readable description of the state machine.

        Raises:
            ValueError: Raised when there are duplicate state names.
        """
        if not self._has_unique_names(start_state):
            raise ValueError(
                "Duplicate names detected in state machine. Names must be unique"
            )

        self.start_state = start_state
        self.comment = comment

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

        if comment := self.comment:
            compiled["Comment"] = comment

        with output_path.open("w") as fp:
            json.dump(compiled, fp)

    def _compile_state(self, state: State, /) -> CompiledState:
        """Compile a state to Amazon States Language.

        Args:
            state: The state to compile.

        Returns:
            The compiled representation of the state.
        """
        compiled: CompiledState = {
            "Type": state.state_type.value,  # type: ignore
        }

        # TODO: Probably there is some nice way to move this class-specific
        # compliation logic to those respective classes
        if isinstance(state, TaskState):
            self._compile_task_state_fields(compiled, state)
        if isinstance(state, PassState):
            self._compile_pass_state_fields(compiled, state)

        self._compile_generic_state_fields(compiled, state)

        return compiled

    @staticmethod
    def _compile_task_state_fields(compiled: CompiledState, state: TaskState) -> None:
        """Compile task state fields.

        Args:
            compiled: The compilation to save new fields to.
            state: The state.
        """
        compiled["Resource"] = state.resource_uri
        if result_selector := state.result_selector:
            compiled["ResultSelector"] = result_selector

    @staticmethod
    def _compile_pass_state_fields(compiled: CompiledState, state: PassState) -> None:
        """Compile pass state fields.

        Args:
            compiled: The compilation to save new fields to.
            state: The state.
        """
        if result := state.result:
            compiled["Result"] = result

    @staticmethod
    def _compile_generic_state_fields(compiled: CompiledState, state: State) -> None:
        """Compile state fields that are common to all states.

        Args:
            compiled: The compilation to save new fields to.
            state: The state.
        """
        if comment := state.comment:
            compiled["Comment"] = comment

        if state.input_path != "$":
            compiled["InputPath"] = state.input_path

        if state.output_path != "$":
            compiled["OutputPath"] = state.output_path

        if state.result_path != "$":
            compiled["ResultPath"] = state.result_path

        if next_state := state.next_state:
            compiled["Next"] = next_state.name
        else:
            compiled["End"] = True

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
        state_input = apply_json_path(state.input_path, state_input)

        # Run the state to get the state output
        if isinstance(state, TaskState):
            state_output = state.run(
                state_input, mock_fn=resource_to_mock_fn[state.resource_uri]
            )
        else:
            state_output = state.run(state_input)

        # Apply the ResultSelector to filter the state output
        # TODO: Add Map, Parallel here
        if isinstance(state, TaskState) and (result_selector := state.result_selector):
            state_output = self._apply_result_selector(state_output, result_selector)

        result_path_output = self._apply_result_path(
            state_input, state_output, state.result_path
        )
        return apply_json_path(state.output_path, result_path_output)

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
