from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple, Union

from awsstepfuncs.abstract_state import AbstractRetryCatchState, AbstractState
from awsstepfuncs.types import ResourceToMockFn
from awsstepfuncs.visualization import Visualization

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
        self.start_state = start_state

        if not self._has_unique_names():
            raise ValueError(
                "Duplicate names detected in state machine. Names must be unique"
            )

        self.comment = comment
        self.version = version

    @property
    def all_states(self) -> Set[AbstractState]:
        """Return all states in the state machine.

        Returns:
            A set of all possible states in the state machine.
        """
        return self._all_states_recursive(self.start_state)

    def _all_states_recursive(self, start_state: AbstractState) -> Set[AbstractState]:
        """Return all states from the given starting state.

        Args:
            start_state: The starting state.

        Returns:
            All possible states from the given starting state.
        """
        all_states = set()
        for state in start_state:
            all_states.add(state)
            if isinstance(state, AbstractRetryCatchState):
                for catcher in state.catchers:
                    all_states |= self._all_states_recursive(catcher.next_state)
        return all_states

    def _has_unique_names(self) -> bool:
        """Check if all states have unique names.

        All state names must be unique in a state machine.

        Returns:
            Whether all states have unique names.
        """
        all_state_names = [state.name for state in self.all_states]
        return len(all_state_names) == len(set(all_state_names))

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile a state machine to Amazon States Language.

        Returns:
            A dictionary of the compiled state in Amazon States Language.
        """
        compiled = {
            "StartAt": self.start_state.name,
            "States": {state.name: state.compile() for state in self.all_states},
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

    def simulate(  # noqa: CCR001
        self,
        *,
        state_input: dict = None,
        resource_to_mock_fn: ResourceToMockFn = None,
        show_visualization: bool = False,
    ) -> Any:
        """Simulate the state machine by executing all of the states.

        Args:
            state_input: Data to pass to the first state.
            resource_to_mock_fn: A dictionary mapping Resource URI to a mock
                function to use in the simulation.
            show_visualization: Whether or not to create an animated GIF
                visualization of the state machine when simulating. Outputs to
                `state_machine.gif`.

        Returns:
            The final output state from simulating the state machine.
        """
        if state_input is None:
            state_input = {}

        if resource_to_mock_fn is None:
            resource_to_mock_fn = {}

        visualization = None
        if show_visualization:
            visualization = Visualization(self.start_state)

        current_data = state_input
        current_state: Optional[AbstractState] = self.start_state
        print("Starting simulation of state machine")

        while current_state is not None:
            print(f"Running {current_state}")
            print("State input:", current_data)
            if visualization:
                visualization.highlight_state(current_state)

            next_state, next_data = self._simulate_state(
                current_state, current_data, resource_to_mock_fn
            )

            if visualization and next_state:
                visualization.highlight_state_transition(current_state, next_state)

            current_state, current_data = next_state, next_data
            print("State output:", current_data)

        if visualization:
            visualization.render()

        print("Terminating simulation of state machine")

        return current_data

    def _simulate_state(
        self,
        state: AbstractState,
        state_input: Any,
        resource_to_mock_fn: ResourceToMockFn,
    ) -> Tuple[Optional[AbstractState], Any]:
        """Simulate a state, while handling input and output data and errors.

        Args:
            state: The current state.
            state_input: The current data (passed as state input).
            resource_to_mock_fn: A dictionary mapping Resource URI to a mock
                function to use in the simulation.

        Returns:
            The next state and the output data.
        """
        try:
            state_output = state.simulate(state_input, resource_to_mock_fn) or {}
        except Exception:
            print("Error encountered in state, checking for catchers")
            next_state = self._check_for_catchers(state)
            state_output = {}
        else:
            next_state = state.next_state

        return next_state, state_output

    @staticmethod
    def _check_for_catchers(state: AbstractState) -> Optional[AbstractState]:
        """Check for any failed state catchers.

        Currently only checks for the "catch-all" catcher of error name
        States.ALL.

        Args:
            state: The state to check for catchers.

        Returns:
            The state to transition to if a catcher can be applied.
        """
        if isinstance(state, AbstractRetryCatchState):
            for catcher in state.catchers:
                if "States.ALL" in catcher.error_equals:
                    print(f"Found catcher, transitioning to {catcher.next_state}")
                    return catcher.next_state
            else:
                print("No catchers were matched")
        return None
