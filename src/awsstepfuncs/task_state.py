from typing import Callable, Optional

from awsstepfuncs.state import State, StateType


class LambdaState(State):
    """Lambda state in Amazon States Language.

    A task state represents a single unit of work performed by a state machine.
    """

    state_type = StateType.LAMBDA

    def __init__(
        self, name: str, /, *, comment: Optional[str] = None, resource_uri: str
    ):
        """Initialize a Lambda state.

        Args:
            name: The name of the state.
            comment: A human-readable description of the state.
            resource_uri: A URI, especially an ARN that uniquely identifies the
                specific task to execute.
        """
        self.resource_uri = resource_uri
        super().__init__(name, comment=comment)

    def run(self, mock_fn: Callable) -> None:  # type: ignore
        """Execute the task state according to Amazon States Language."""
        mock_fn()
