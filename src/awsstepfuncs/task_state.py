from typing import Callable, Optional

from awsstepfuncs.state import State, StateType


class TaskState(State):
    """Task state in Amazon States Language.

    A task state represents a single unit of work performed by a state machine.
    """

    state_type = StateType.TASK

    def __init__(
        self, name: str, /, *, comment: Optional[str] = None, resource_uri: str
    ):
        """Initialize a task state.

        Args:
            name: The name of the state.
            comment: A human-readable description of the state.
            resource_uri: A URI, especially an ARN that uniquely identifies the
                specific task to execute.
        """
        self.resource_uri = resource_uri
        super().__init__(name, comment=comment)

    def run(self, state_input: dict, mock_fn: Callable) -> dict:  # type: ignore
        """Execute the task state according to Amazon States Language.

        Args:
            state_input: The input state data to be passed to the mock function.
            mock_fn: The mock function to run for the simulation.

        Returns:
            The output state from running the mock function.
        """
        return mock_fn(state_input)
