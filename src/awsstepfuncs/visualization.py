import os
from tempfile import TemporaryDirectory
from typing import Optional

import gvanim

from awsstepfuncs.abstract_state import AbstractRetryCatchState, AbstractState


class Visualization:
    """Create a visualization of a state machine."""

    def __init__(
        self,
        start_state: AbstractState,
    ):
        """Initialize a state machine visualization.

        Args:
            start_state: The starting state of the state machine, used to
                determine all possible state transitions.
        """
        self.animation = gvanim.Animation()
        self._build_state_graph(start_state)

    def _build_state_graph(self, start_state: AbstractState) -> None:  # noqa: CCR001
        """Add all the possible state transitions to the graph.

        Args:
            start_state: The starting state of the state machine, used to
                determine all possible state transitions.
        """
        # TODO: Check if there's a way to refactor the code to avoid this
        # circular dependency
        from awsstepfuncs.state import ChoiceState

        self.animation.add_node(start_state.name)
        current_state: Optional[AbstractState] = start_state
        while current_state is not None:
            if current_state.next_state is not None:
                self.animation.add_edge(
                    current_state.name, current_state.next_state.name
                )
            elif isinstance(current_state, ChoiceState):
                for choice in current_state.choices:
                    self.animation.add_edge(current_state.name, choice.next_state.name)
                    self._build_state_graph(choice.next_state)

                if default := current_state.default:
                    self.animation.add_edge(current_state.name, default.name)
                    self._build_state_graph(default)

            if isinstance(current_state, AbstractRetryCatchState):
                for catcher in current_state.catchers:
                    self.animation.add_edge(current_state.name, catcher.next_state.name)
                    self._build_state_graph(catcher.next_state)

            current_state = current_state.next_state

    def render(self) -> None:
        """Render the state machine visualization to a file (`state_machine.gif`)."""
        graphs = self.animation.graphs()
        base_name = "state_machine"
        size = 700
        with TemporaryDirectory() as tmp_dir:
            files = gvanim.render(
                graphs, os.path.join(tmp_dir, base_name), "png", size=size
            )
            gvanim.gif(files, base_name, delay=50, size=size)

    def highlight_state(self, state: AbstractState) -> None:
        """Highlight a state.

        Args:
            state: The state to highlight, unique by name.
        """
        self.animation.highlight_node(state.name)

    def highlight_state_transition(
        self, previous_state: AbstractState, next_state: AbstractState
    ) -> None:
        """Highlight the transition between two states.

        Args:
            previous_state: The previous state.
            next_state: The next state.
        """
        self.animation.next_step()
        self.animation.highlight_edge(previous_state.name, next_state.name)
        self.animation.next_step()
