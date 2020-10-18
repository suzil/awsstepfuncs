from typing import Any, Callable, Dict, Optional

from awsstepfuncs.json_path import validate_json_path
from awsstepfuncs.state import State, StateType


class TaskState(State):
    """Task state in Amazon States Language.

    A task state represents a single unit of work performed by a state machine.
    """

    state_type = StateType.TASK

    def __init__(
        self,
        name: str,
        /,
        *,
        comment: Optional[str] = None,
        input_path: str = "$",
        output_path: str = "$",
        result_path: Optional[str] = "$",
        resource_uri: str,
        result_selector: Optional[Dict[str, str]] = None,
    ):
        """Initialize a task state.

        Args:
            name: The name of the state.
            comment: A human-readable description of the state.
            input_path: Used to select a portion of the state input. Default is
                $ (pass everything).
            output_path: Used to select a portion of the state output. Default
                is $ (pass everything).
            result_path: Specifies where (in the input) to place the "output" of
                the virtual task specified in Result. The input is further filtered
                as specified by the OutputPath field (if present) before being used
                as the state's output. Default is $ (pass only the output state).
            resource_uri: A URI, especially an ARN that uniquely identifies the
                specific task to execute.
            result_selector: Used to manipulate a state's result before
                ResultPath is applied.

        Raises:
            ValueError: Raised when the result selector is an invalid.
            ValueError: Raised when an invalid JSONPath is specified.
        """
        self.resource_uri = resource_uri

        if result_selector:
            try:
                self._validate_result_selector(result_selector)
            except ValueError:
                raise

        self.result_selector = result_selector
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
        )

    @staticmethod
    def _validate_result_selector(result_selector: Dict[str, str]) -> None:
        """Validate result selector.

        Here is a valid result selector:

        >>> TaskState._validate_result_selector({"ClusterId.$": "$.output.ClusterId", "ResourceType.$": "$.resourceType"})

        Result selector keys must end with ".$".

        >>> TaskState._validate_result_selector({"ClusterId": "$.output.ClusterId"})
        Traceback (most recent call last):
            ...
        ValueError: All resource selector keys must end with .$

        Values must be valid JSONPaths.

        >>> TaskState._validate_result_selector({"ClusterId.$": "something invalid"})
        Traceback (most recent call last):
            ...
        ValueError: JSONPath must begin with "$"

        Args:
            result_selector: The result selector to validate.

        Raises:
            ValueError: Raised when a key doesn't end with ".$".
            ValueError: Raised when a JSONPath is invalid.
        """
        for key, json_path in result_selector.items():
            if not key[-2:] == ".$":
                raise ValueError("All resource selector keys must end with .$")

            try:
                validate_json_path(json_path)
            except ValueError:
                raise

    def run(self, state_input: Any, mock_fn: Callable) -> Any:  # type: ignore
        """Execute the task state according to Amazon States Language.

        Args:
            state_input: The input state data to be passed to the mock function.
            mock_fn: The mock function to run for the simulation.

        Returns:
            The output state from running the mock function.
        """
        return mock_fn(state_input)
