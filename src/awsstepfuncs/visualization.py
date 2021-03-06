import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Union

import gvanim

from awsstepfuncs.abstract_state import AbstractState
from awsstepfuncs.errors import AWSStepFuncsValueError


class Visualization:
    """Create a visualization of a state machine.

    Here's a few examples of visualizations using
    `show_visualization=True`:

    .. highlight:: python
    .. code-block:: python

        resource = "123"
        task_state = TaskState("My task", resource=resource)
        succeed_state = SucceedState("Success")
        pass_state = PassState("Just passing")
        fail_state = FailState("Failure", error="IFailed", cause="I failed!")

        task_state >> succeed_state
        pass_state >> fail_state

        task_state.add_catcher(["States.ALL"], next_state=pass_state)

        state_machine = StateMachine(start_state=task_state)

        def failure_mock_fn(event, context):
            assert False

        state_machine.simulate(
            resource_to_mock_fn={resource: failure_mock_fn}, show_visualization=True
        )

    .. figure:: ../../../assets/state_machine.gif

       state_machine.gif

    A choice state:

    .. highlight:: python
    .. code-block:: python

        public_state = PassState("Public")
        value_in_twenties_state = PassState("ValueInTwenties")
        after_value_in_twenties_state = SucceedState("Success!")
        start_audit_state = PassState("StartAudit")

        value_in_twenties_state >> after_value_in_twenties_state

        choice_state = ChoiceState(
            "DispatchEvent",
            choices=[
                NotChoice(
                    variable="$.type",
                    string_equals="Private",
                    next_state=public_state,
                ),
                AndChoice(
                    [
                        ChoiceRule(variable="$.value", is_present=True),
                        ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
                        ChoiceRule(variable="$.value", numeric_less_than=30),
                    ],
                    next_state=value_in_twenties_state,
                ),
                VariableChoice(
                    variable="$.rating",
                    numeric_greater_than_path="$.auditThreshold",
                    next_state=start_audit_state,
                )
            ],
        )

        state_machine = StateMachine(start_state=choice_state)
        state_machine.simulate(
            {"type": "Private", "value": 22}, show_visualization=True
        )

    .. figure:: ../../../assets/choice_visualization.gif

       state_machine.gif

    A choice state when the default is chosen:

    .. highlight:: python
    .. code-block:: python

        record_event_state = PassState("RecordEvent")

        choice_state = ChoiceState(
            "DispatchEvent",
            choices=[
                NotChoice(
                    variable="$.type",
                    string_equals="Private",
                    next_state=public_state,
                ),
                AndChoice(
                    [
                        ChoiceRule(variable="$.value", is_present=True),
                        ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
                        ChoiceRule(variable="$.value", numeric_less_than=30),
                    ],
                    next_state=value_in_twenties_state,
                ),
                VariableChoice(
                    variable="$.rating",
                    numeric_greater_than_path="$.auditThreshold",
                    next_state=start_audit_state,
                )
            ],
            default=record_event_state,
        )
        state_machine = StateMachine(start_state=choice_state)
        state_machine.simulate(
            {"type": "Private", "value": 102, "auditThreshold": 150},
            show_visualization=True
        )

    .. figure:: ../../../assets/default_choice_visualization.gif

       state_machine.gif
    """

    def __init__(
        self,
        *,
        start_state: AbstractState,
        output_path: Union[str, Path] = "state_machine.gif",
    ):
        """Initialize a state machine visualization.

        Make sure that if you specify the output path for the visualization that it
        ends with `.gif`; otherwise, there will be an error.

        Args:
            start_state: The starting state of the state machine, used to
                determine all possible state transitions.
            output_path: What path to save the visualization GIF to.

        Raises:
            AWSStepFuncsValueError: Raised when the output path doesn't end with `.gif`.
        """
        self.animation = gvanim.Animation()
        self._build_state_graph(start_state)

        if not str(output_path).endswith(".gif"):
            raise AWSStepFuncsValueError(
                'Visualization output path must end with ".gif"'
            )

        self.output_path = Path(output_path)

    def _build_state_graph(self, start_state: AbstractState) -> None:  # noqa: CCR001
        """Add all the possible state transitions to the graph.

        Args:
            start_state: The starting state of the state machine, used to
                determine all possible state transitions.
        """
        self.animation.add_node(start_state.name)
        current_state: Optional[AbstractState] = start_state
        while current_state is not None:
            if current_state.next_state is not None:
                self.animation.add_edge(
                    current_state.name, current_state.next_state.name
                )
            elif hasattr(current_state, "choices"):
                for choice in current_state.choices:  # type: ignore
                    self.animation.add_edge(current_state.name, choice.next_state.name)
                    self._build_state_graph(choice.next_state)

                if default := current_state.default:  # type: ignore
                    self.animation.add_edge(current_state.name, default.name)
                    self._build_state_graph(default)

            if hasattr(current_state, "catchers"):
                for catcher in current_state.catchers:  # type: ignore
                    self.animation.add_edge(current_state.name, catcher.next_state.name)
                    self._build_state_graph(catcher.next_state)

            current_state = current_state.next_state

    def render(self) -> None:
        """Render the state machine visualization to a GIF file."""
        graphs = self.animation.graphs()
        size = 700
        with TemporaryDirectory() as tmp_dir:
            files = gvanim.render(
                graphs, os.path.join(tmp_dir, "state_machine"), "png", size=size
            )
            # TODO: Replace with removesuffix() when dropping 3.8 support
            output_path_without_ext = str(self.output_path)[:-4]
            gvanim.gif(files, output_path_without_ext, delay=50, size=size)

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
