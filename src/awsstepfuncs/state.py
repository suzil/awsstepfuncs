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
from typing import Any, Callable, Dict, List, Optional

from awsstepfuncs.json_path import validate_json_path

MAX_STATE_NAME_LENGTH = 128


class AbstractState(ABC):
    """An Amazon States Language state including Name, Comment, and Type."""

    def __init__(self, name: str, comment: Optional[str] = None):
        """Initialize subclasses.

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
        self.next_state: Optional[AbstractState] = None

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        assert self.state_type  # type: ignore
        compiled = {"Type": self.state_type}  # type: ignore
        if comment := self.comment:
            compiled["Comment"] = comment
        return compiled

    @abstractmethod
    def run(self, state_input: Any) -> Any:
        """Execute the state.

        Args:
            state_input: The input state data.

        Raises:
            NotImplementedError: Raised if not implemented by subclasses.
        """
        raise NotImplementedError

    def __rshift__(self, other: AbstractState, /) -> AbstractState:
        """Overload >> operator when state execution order.

        Args:
            other: The other state besides self.

        Returns:
            The latest state (for right shift, the right state).
        """
        # TODO: Add validation as Choice, Succeed, Fail cannot have a "next"
        self.next_state = other
        return other

    def __iter__(self) -> AbstractState:
        """Iterate through the states."""
        self._current: Optional[AbstractState] = self
        return self._current

    def __next__(self) -> AbstractState:
        """Get the next state."""
        current = self._current
        if not current:
            raise StopIteration

        self._current = current.next_state
        return current


class FailState(AbstractState):
    """The Fail State terminates the machine and marks it as a failure.

    >>> fail_state = FailState("FailState", error="ErrorA", cause="Kaiju attack")
    >>> fail_state.compile()
    {'Type': 'Fail', 'Error': 'ErrorA', 'Cause': 'Kaiju attack'}
    """

    state_type = "Fail"

    def __init__(self, *args: Any, error: str, cause: str, **kwargs: Any):
        """Initialize a Fail State.

        Args:
            args: Args to pass to parent classes.
            error: The name of the error.
            cause: A human-readable error message.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.error = error
        self.cause = cause

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        compiled["Error"] = self.error
        compiled["Cause"] = self.cause
        return compiled

    def run(self, state_input: Any) -> None:
        """Execute the Fail State according to Amazon States Language.

        Args:
            state_input: The input state data.
        """
        return


class AbstractInputPathOutputPathState(AbstractState):
    """An Amazon States Language state including InputPath and OutputPath."""

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
            kwargs: Kwargs to pass to parent classes.

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
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if (input_path := self.input_path) != "$":
            compiled["InputPath"] = input_path
        if (output_path := self.output_path) != "$":
            compiled["OutputPath"] = output_path
        return compiled


class SucceedState(AbstractInputPathOutputPathState):
    """The Succeed State terminates with a mark of success.

    The branch can either be:
        - The entire state machine
        - A branch of a Parallel State
        - An iteration of a Map State
    """

    state_type = "Succeed"


class ChoiceState(AbstractInputPathOutputPathState):
    """A Choice State adds branching logic to a state machine."""

    state_type = "Choice"


class AbstractNextOrEndState(AbstractInputPathOutputPathState):
    """An Amazon States Language state including Next or End."""

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if next_state := self.next_state:
            compiled["Next"] = next_state.name
        else:
            compiled["End"] = True
        return compiled


class WaitState(AbstractNextOrEndState):
    """A Wait State causes the interpreter to delay the machine for a specified time."""

    state_type = "Wait"


class AbstractResultPathState(AbstractNextOrEndState):
    """An Amazon States Language state including ResultPath."""

    def __init__(self, *args: Any, result_path: Optional[str] = "$", **kwargs: Any):
        """Initialize subclasses.

        Args:
            args: Args to pass to parent classes.
            result_path: Specifies where (in the input) to place the "output" of
                the virtual task specified in Result. The input is further filtered
                as specified by the OutputPath field (if present) before being used
                as the state's output. Default is $ (pass only the output state).
            kwargs: Kwargs to pass to parent classes.

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
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if (result_path := self.result_path) != "$":
            compiled["ResultPath"] = result_path
        return compiled


class AbstractParametersState(AbstractResultPathState):
    """An Amazon States Language state includin Parameters."""

    def __init__(
        self, *args: Any, parameters: Optional[Dict[str, Any]] = None, **kwargs: Any
    ):
        """Initialize subclasses.

        Args:
            args: Args to pass to parent classes.
            parameters: Use the Parameters field to create a collection of
                key-value pairs that are passed as input. The values of each can
                either be static values that you include in your state machine
                definition, or selected from either the input or the context object
                with a path. For key-value pairs where the value is selected using a
                path, the key name must end in .$.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.parameters = parameters or {}

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if parameters := self.parameters:
            compiled["Parameters"] = parameters
        return compiled


class PassState(AbstractParametersState):
    """The Pass State by default passes its input to its output, performing no work."""

    state_type = "Pass"

    def __init__(self, *args: Any, result: Any = None, **kwargs: Any):
        """Initialize a Pass State.

        Args:
            args: Args to pass to parent classes.
            result: If present, its value is treated as the output of a virtual
                task, and placed as prescribed by the "ResultPath" field.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.result = result

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if result := self.result:
            compiled["Result"] = result
        return compiled

    def run(self, state_input: Any) -> Any:
        """Execute the Pass State according to Amazon States Language.

        Args:
            state_input: The input state data. For the Pass State, it will
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
    """An Amazon States Language state including ResultSelector."""

    def __init__(
        self, *args: Any, result_selector: Dict[str, str] = None, **kwargs: Any
    ):
        """Initialize subclasses.

        Args:
            args: Args to pass to parent classes.
            result_selector: Used to manipulate a state's result before
                ResultPath is applied.
            kwargs: Kwargs to pass to parent classes.

        Raises:
            ValueError: Raised when the result selector is invalid.
        """
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
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if result_selector := self.result_selector:
            compiled["ResultSelector"] = result_selector
        return compiled


class AbstractRetryCatchState(AbstractResultSelectorState):
    """An Amazon States Language state including Retry and Catch."""

    def __init__(
        self,
        *args: Any,
        retry: List[Dict[str, Any]] = None,
        catch: List[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """Initialize subclasses.

        TODO: Retriers and catches should be specified in a programmatic way.

        Args:
            args: Args to pass to parent classes.
            retry: Retry the state by specifying a list of Retriers that
                describes the retry policy for different errors.
            catch: When a state reports an error and either there is no Retrier,
                or retries have failed to resolve the error, the interpreter will
                try to find a relevant Catcher which determines which state to
                transition to.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.retry = retry
        self.catch = catch

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if retry := self.retry:
            compiled["Retry"] = retry
        if catch := self.catch:
            compiled["Catch"] = catch
        return compiled


class TaskState(AbstractRetryCatchState):
    """The Task State executes the work identified by the Resource field."""

    state_type = "Task"

    def __init__(self, *args: Any, resource: str, **kwargs: Any):
        """Initialize a Task State.

        Args:
            args: Args to pass to parent classes.
            resource: A URI, especially an ARN that uniquely identifies the
                specific task to execute.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.resource = resource

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        compiled["Resource"] = self.resource
        return compiled

    def run(self, state_input: Any, mock_fn: Callable) -> Any:  # type: ignore
        """Execute the Task State according to Amazon States Language.

        Args:
            state_input: The input state data to be passed to the mock function.
            mock_fn: The mock function to run for the simulation.

        Returns:
            The output state from running the mock function.
        """
        return mock_fn(state_input)


class ParallelState(AbstractRetryCatchState):
    """The Parallel State causes parallel execution of branches."""

    state_type = "Parallel"


class MapState(AbstractRetryCatchState):
    """The Map State processes all the elements of an array."""

    state_type = "Map"
