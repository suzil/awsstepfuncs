import os
from tempfile import TemporaryDirectory
from typing import Optional

import gvanim

from awsstepfuncs.abstract_state import AbstractRetryCatchState, AbstractState


class Visualization:
    """Create a visualization of a state machine.

    Here's a few examples of visualizations using
    `show_visualization=True`:

    >>> from awsstepfuncs import *

    >>> resource = "123"
    >>> task_state = TaskState("My task", resource=resource)
    >>> succeed_state = SucceedState("Success")
    >>> pass_state = PassState("Just passing")
    >>> fail_state = FailState("Failure", error="IFailed", cause="I failed!")
    >>> _ = task_state >> succeed_state
    >>> _ = pass_state >> fail_state
    >>> _ = task_state.add_catcher(["States.ALL"], next_state=pass_state)
    >>> state_machine = StateMachine(start_state=task_state)
    >>> def failure_mock_fn(_):
    ...     assert False
    >>> _ = state_machine.simulate(
    ...     resource_to_mock_fn={resource: failure_mock_fn}, show_visualization=True
    ... )
    Starting simulation of state machine
    Running TaskState('My task')
    State input: {}
    State input after applying input path of $: {}
    Error encountered in state, checking for catchers
    Found catcher, transitioning to PassState('Just passing')
    State output: {}
    Running PassState('Just passing')
    State input: {}
    State input after applying input path of $: {}
    Output from applying result path of $: {}
    State output after applying output path of $: {}
    State output: {}
    Running FailState('Failure', error='IFailed', cause='I failed!')
    State input: {}
    State output: {}
    Terminating simulation of state machine

    .. figure:: ../../../assets/state_machine.gif

       state_machine.gif

    A choice state:

    >>> public_state = PassState("Public")
    >>> value_in_twenties_state = PassState("ValueInTwenties")
    >>> after_value_in_twenties_state = SucceedState("Success!")
    >>> _ = value_in_twenties_state >> after_value_in_twenties_state
    >>> start_audit_state = PassState("StartAudit")
    >>> record_event_state = PassState("RecordEvent")
    >>> choice_state = ChoiceState(
    ...     "DispatchEvent",
    ...     choices=[
    ...         NotChoice(
    ...             variable="$.type",
    ...             string_equals="Private",
    ...             next_state=public_state,
    ...         ),
    ...         AndChoice(
    ...             [
    ...                 ChoiceRule(variable="$.value", is_present=True),
    ...                 ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
    ...                 ChoiceRule(variable="$.value", numeric_less_than=30),
    ...             ],
    ...             next_state=value_in_twenties_state,
    ...         ),
    ...         VariableChoice(
    ...             variable="$.rating",
    ...             numeric_greater_than_path="$.auditThreshold",
    ...             next_state=start_audit_state,
    ...         )
    ...     ],
    ... )
    >>> state_machine = StateMachine(start_state=choice_state)
    >>> _ = state_machine.simulate(
    ...     state_input={"type": "Private", "value": 22}, show_visualization=True
    ... )
    Starting simulation of state machine
    Running ChoiceState('DispatchEvent')
    State input: {'type': 'Private', 'value': 22}
    State input after applying input path of $: {'type': 'Private', 'value': 22}
    State output after applying output path of $: {'type': 'Private', 'value': 22}
    State output: {'type': 'Private', 'value': 22}
    Running PassState('ValueInTwenties')
    State input: {'type': 'Private', 'value': 22}
    State input after applying input path of $: {'type': 'Private', 'value': 22}
    Output from applying result path of $: {'type': 'Private', 'value': 22}
    State output after applying output path of $: {'type': 'Private', 'value': 22}
    State output: {'type': 'Private', 'value': 22}
    Running SucceedState('Success!')
    State input: {'type': 'Private', 'value': 22}
    State input after applying input path of $: {'type': 'Private', 'value': 22}
    State output after applying output path of $: {'type': 'Private', 'value': 22}
    State output: {'type': 'Private', 'value': 22}
    Terminating simulation of state machine

    .. figure:: ../../../assets/choice_visualization.gif

       state_machine.gif

    A choice state when the default is chosen:

    >>> choice_state = ChoiceState(
    ...     "DispatchEvent",
    ...     choices=[
    ...         NotChoice(
    ...             variable="$.type",
    ...             string_equals="Private",
    ...             next_state=public_state,
    ...         ),
    ...         AndChoice(
    ...             [
    ...                 ChoiceRule(variable="$.value", is_present=True),
    ...                 ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
    ...                 ChoiceRule(variable="$.value", numeric_less_than=30),
    ...             ],
    ...             next_state=value_in_twenties_state,
    ...         ),
    ...         VariableChoice(
    ...             variable="$.rating",
    ...             numeric_greater_than_path="$.auditThreshold",
    ...             next_state=start_audit_state,
    ...         )
    ...     ],
    ...     default=record_event_state,
    ... )
    >>> state_machine = StateMachine(start_state=choice_state)
    >>> _ = state_machine.simulate(
    ...     state_input={"type": "Private", "value": 102, "auditThreshold": 150},
    ...     show_visualization=True
    ... )
    Starting simulation of state machine
    Running ChoiceState('DispatchEvent')
    State input: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
    State input after applying input path of $: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
    No choice evaluated to true
    Choosing next state by the default set
    State output after applying output path of $: {}
    State output: {}
    Running PassState('RecordEvent')
    State input: {}
    State input after applying input path of $: {}
    Output from applying result path of $: {}
    State output after applying output path of $: {}
    State output: {}
    Terminating simulation of state machine

    .. figure:: ../../../assets/default_choice_visualization.gif

       state_machine.gif
    """

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
