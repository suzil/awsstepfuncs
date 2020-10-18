"""State definitions.

Class structure based on this table: https://states-language.net/spec.html#state-type-table

Each row in the table is its own ABC. All latter rows inherit from previous
rows. States (the columns) are concrete classes that inherit from the ABC that
has the right fields available.

Each concrete class should implement its own run() method that will run the
state according to its business logic when running a simulation. Each concrete
class should also define a constant class variable called `state_type` that
corresponds to type in Amazon States Language.

TODO: Right now the iterable for passing through states is on the level of
`AbstractNextOrEndState` as that is the state that has the `next_state` attribute,
but it's unclear how other states should be incorporated into the state machine
without using `next_state`. It might be a case where we need to deviate from the
language specification in our implementation and move `next_state` to
`AbstractState`.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional

from awsstepfuncs.json_path import validate_json_path

MAX_STATE_NAME_LENGTH = 128


class AbstractState(ABC):
    """An AWS Step Functions state."""

    def __init__(self, name: str, comment: Optional[str] = None):
        """Initialize a state.

        Args:
            name: The name of the state. Must be unique within the state machine
                and its length cannot exceed 128 characters.
            comment: A human-readable description of the state.

        Raises:
            ValueError: Raised when the state name exceeds 128 characters.
        """
        if len(name) > MAX_STATE_NAME_LENGTH:
            raise ValueError(
                f'State name "{name}" must be less than {MAX_STATE_NAME_LENGTH} characters'
            )

        self.name = name
        self.comment = comment

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        assert self.state_type  # type: ignore
        compiled = {"Type": self.state_type}  # type: ignore
        if comment := self.comment:
            compiled["Comment"] = comment
        return compiled

    @abstractmethod
    def run(self, state_input: dict) -> dict:
        """Execute the state.

        Args:
            state_input: The input state data.

        Raises:
            NotImplementedError: Raised if not implemented by subclasses.
        """
        raise NotImplementedError


class FailState(AbstractState):
    state_type = "Fail"


class AbstractInputPathOutputPathState(AbstractState):
    """Includes InputPath and OutputPath."""

    def __init__(
        self, *args: Any, input_path: str = "$", output_path: str = "$", **kwargs: Any
    ):
        """Initialize subclasses.

        Args:
            args: Args to pass to parent classes.
            input_path: Used to select a portion of the state input. Default is
                $ (pass everything).
            output_path: Used to select a portion of the state output. Default
                is $ (pass everything).
            kwargs: Kwargs to pass ot parent classes.

        Raises:
            ValueError: Raised when an invalid JSONPath is specified.
        """
        super().__init__(*args, **kwargs)
        for json_path in [input_path, output_path]:
            try:
                validate_json_path(json_path)
            except ValueError:
                raise

        self.input_path = input_path
        self.output_path = output_path

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if (input_path := self.input_path) != "$":
            compiled["InputPath"] = input_path
        if (output_path := self.output_path) != "$":
            compiled["OutputPath"] = output_path
        return compiled


class SucceedState(AbstractInputPathOutputPathState):
    state_type = "Succeed"


class ChoiceState(AbstractInputPathOutputPathState):
    state_type = "Choice"


class AbstractNextOrEndState(AbstractInputPathOutputPathState):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.next_state: Optional[AbstractNextOrEndState] = None

    def __rshift__(self, other: AbstractNextOrEndState, /) -> AbstractNextOrEndState:
        """Overload >> operator when state execution order.

        Args:
            other: The other state besides self.

        Returns:
            The latest state (for right shift, the right state).
        """
        self.next_state = other
        return other

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if next_state := self.next_state:
            compiled["Next"] = next_state.name
        else:
            compiled["End"] = True
        return compiled

    def __iter__(self) -> AbstractNextOrEndState:
        """Iterate through the states."""
        self._current: Optional[AbstractNextOrEndState] = self
        return self._current

    def __next__(self) -> AbstractNextOrEndState:
        """Get the next state."""
        current = self._current
        if not current:
            raise StopIteration

        self._current = current.next_state
        return current


class WaitState(AbstractNextOrEndState):
    state_type = "Wait"


class AbstractResultPathState(AbstractNextOrEndState):
    def __init__(self, *args: Any, result_path: Optional[str] = "$", **kwargs: Any):
        """Initialize subclasses.

        Args:
            args: Args to pass to parent classes.
            result_path: Specifies where (in the input) to place the "output" of
                the virtual task specified in Result. The input is further filtered
                as specified by the OutputPath field (if present) before being used
                as the state's output. Default is $ (pass only the output state).
            kwargs: Kwargs to pass ot parent classes.

        Raises:
            ValueError: Raised when the result_path is an invalid JSONPath.
        """
        super().__init__(*args, **kwargs)
        if result_path:
            try:
                validate_json_path(result_path)
            except ValueError:
                raise

        self.result_path = result_path

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if (result_path := self.result_path) != "$":
            compiled["ResultPath"] = result_path
        return compiled


class AbstractParametersState(AbstractResultPathState):
    def __init__(
        self, *args: Any, parameters: Optional[Dict[str, Any]] = None, **kwargs: Any
    ):
        super().__init__(*args, **kwargs)
        self.parameters = parameters or {}

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if parameters := self.parameters:
            compiled["Parameters"] = parameters
        return compiled


class PassState(AbstractParametersState):
    state_type = "Pass"

    def __init__(self, *args: Any, result: Any = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.result = result

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if result := self.result:
            compiled["Result"] = result
        return compiled

    def run(self, state_input: dict) -> dict:
        """Execute the pass state according to Amazon States Language.

        Args:
            state_input: The input state data. For the pass state, it will
                simply be passed as the output state with no transformation.

        Returns:
            The output state, same as the input state in this case.
        """
        print("Passing")  # noqa: T001
        if result := self.result:
            return result
        else:
            return state_input


class AbstractResultSelectorState(AbstractParametersState):
    def __init__(
        self, *args: Any, result_selector: Dict[str, str] = None, **kwargs: Any
    ):
        super().__init__(*args, **kwargs)
        if result_selector:
            try:
                self._validate_result_selector(result_selector)
            except ValueError:
                raise

        self.result_selector = result_selector

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

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if result_selector := self.result_selector:
            compiled["ResultSelector"] = result_selector
        return compiled


class AbstractRetryCatchState(AbstractResultSelectorState):
    # TODO: Types for retry and catch
    def __init__(self, *args: Any, retry: Any = None, catch: Any = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.retry = retry
        self.catch = catch

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        if retry := self.retry:
            compiled["Retry"] = retry
        if catch := self.catch:
            compiled["Catch"] = catch
        return compiled


class TaskState(AbstractRetryCatchState):
    state_type = "Task"

    def __init__(self, *args: Any, resource: str, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.resource = resource

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        compiled = super().compile()
        compiled["Resource"] = self.resource
        return compiled

    def run(self, state_input: Any, mock_fn: Callable) -> Any:  # type: ignore
        """Execute the task state according to Amazon States Language.

        Args:
            state_input: The input state data to be passed to the mock function.
            mock_fn: The mock function to run for the simulation.

        Returns:
            The output state from running the mock function.
        """
        return mock_fn(state_input)


class ParallelState(AbstractRetryCatchState):
    state_type = "Parallel"


class MapState(AbstractRetryCatchState):
    state_type = "Map"
