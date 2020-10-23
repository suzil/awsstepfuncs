"""State definitions.

Class structure based on this table: https://states-language.net/spec.html#state-type-table

Each row in the table is its own ABC. All latter rows inherit from previous
rows. States (the columns) are concrete classes that inherit from the ABC that
has the right fields available.

Each concrete class should implement its own run() method that will run the
state according to its business logic when running a simulation. Each concrete
class should also define a constant class variable called `state_type` that
corresponds to type in Amazon States Language.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from awsstepfuncs.json_path import apply_json_path, validate_json_path
from awsstepfuncs.types import ResourceToMockFn

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
    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the state.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            NotImplementedError: Raised if not implemented by subclasses.
        """
        raise NotImplementedError

    def simulate(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Simulate the state including input and output processing.

        Args:
            state_input: The input to the state.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state after applying any output processing.
        """
        print(f"Running {self.name}")  # noqa: T001
        return self._run(state_input, resource_to_mock_fn)

    def __rshift__(self, other: AbstractState, /) -> AbstractState:
        """Overload >> operator to set state execution order.

        >>> pass_state = PassState("Pass")
        >>> fail_state = FailState("Fail", error="JustBecause", cause="Because I feel like it")
        >>> _ = pass_state >> fail_state
        >>> assert pass_state.next_state is fail_state

        You cannot set a next state on a ChoiceState, SucceedState, or FailState
        as they are terminal states.

        >>> fail_state >> pass_state
        Traceback (most recent call last):
            ...
        ValueError: FailState cannot have a next state

        Args:
            other: The other state besides self.

        Raises:
            ValueError: Raised when trying to set next state on a terminal
                state.

        Returns:
            The latest state (for right shift, the right state).
        """
        for terminal_state in [ChoiceState, SucceedState, FailState]:
            if isinstance(self, terminal_state):
                raise ValueError(f"{terminal_state.__name__} cannot have a next state")

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

    def __repr__(self) -> str:
        """Create a string representation of a state.

        >>> pass_state = PassState("Pass", comment="This is a pass state")
        >>> fail_state = FailState("Fail", error="MyBad", cause="It's a problem")
        >>> _ = pass_state >> fail_state
        >>> pass_state
        PassState(name='Pass', comment='This is a pass state', next_state='Fail')
        >>> fail_state
        FailState(name='Fail')

        Returns:
            String representation of a state.
        """
        state_repr = f"{self.__class__.__name__}(name={self.name!r}"
        if self.comment:
            state_repr += f", comment={self.comment!r}"
        if self.next_state:
            state_repr += f", next_state={self.next_state.name!r}"
        state_repr += ")"
        return state_repr


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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> None:
        """Run the Fail State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.
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

    def simulate(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Simulate the state including input and output processing.

        Args:
            state_input: The input to the state.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state after applying any output processing.
        """
        print(f"Running {self.name}")  # noqa: T001
        state_input = apply_json_path(self.input_path, state_input)
        state_output = self._run(state_input, resource_to_mock_fn)
        return apply_json_path(self.output_path, state_output)

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

    def simulate(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Simulate the state including input and output processing.

        Args:
            state_input: The input to the state.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state after applying any output processing.
        """
        print(f"Running {self.name}")  # noqa: T001
        state_input = apply_json_path(self.input_path, state_input)
        state_output = self._run(state_input, resource_to_mock_fn)
        state_output = self._apply_result_path(state_input, state_output)
        return apply_json_path(self.output_path, state_output)

    def _apply_result_path(self, state_input: Any, state_output: Any) -> Any:
        """Apply ResultPath to combine state input with state output.

        Args:
            state_input: The input state.
            state_output: The output state.

        Returns:
            The state resulting from applying ResultPath.
        """
        if self.result_path == "$":
            # Just keep state output
            return state_output

        elif self.result_path is None:
            # Just keep state input, discard state_output
            return state_input

        elif match := re.fullmatch(r"\$\.([A-Za-z]+)", self.result_path):
            # Move the state output as a key in state input
            result_key = match.group(1)
            state_input[result_key] = state_output
            return state_input

        else:  # pragma: no cover
            assert False, "Should never happen"  # noqa: PT015


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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the Pass State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state, same as input if result is not provided.
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

    def simulate(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Simulate the state including input and output processing.

        Args:
            state_input: The input to the state.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state after applying any output processing.
        """
        print(f"Running {self.name}")  # noqa: T001
        state_input = apply_json_path(self.input_path, state_input)
        state_output = self._run(state_input, resource_to_mock_fn)
        if self.result_selector:
            state_output = self._apply_result_selector(state_output)
        state_output = self._apply_result_path(state_input, state_output)
        return apply_json_path(self.output_path, state_output)

    def _apply_result_selector(self, state_output: Any) -> Dict[str, Any]:
        """Apply the ResultSelector to select a portion of the state output.

        Args:
            state_output: The state output to filter.

        Returns:
            The filtered state output.
        """
        new_state_output = {}
        for key, json_path in self.result_selector.items():  # type: ignore
            key = key[:-2]  # Strip ".$"
            if extracted := apply_json_path(json_path, state_output):
                new_state_output[key] = extracted

        return new_state_output


@dataclass
class Retrier:
    """Used to retry a failed state given the error names."""

    error_equals: List[str]
    interval_seconds: Optional[int] = None
    backoff_rate: Optional[float] = None
    max_attempts: Optional[int] = None

    def __post_init__(self) -> None:
        """Run validation on input values.

        Raises:
            ValueError: Raised when interval_seconds is negative.
            ValueError: Raised when backoff_rate is less than 1.0.
            ValueError: Raised when max_attempts is negative.
        """
        if self.interval_seconds and self.interval_seconds <= 0:  # pragma: no cover
            raise ValueError("interval_seconds must be a positive integer")
        if self.backoff_rate and self.backoff_rate < 1:  # pragma: no cover
            raise ValueError("backoff_rate must be greater than or equal to 1.0")
        if self.max_attempts is not None and self.max_attempts < 0:  # pragma: no cover
            raise ValueError("max_attempts must be zero or a positive integer")

    def compile(self) -> Dict[str, Union[List[str], int, float]]:  # noqa: A003
        """Compile the Retrier to Amazon States Language.

        Returns:
            A Retrier in Amazon States Language.
        """
        compiled: Dict[str, Union[List[str], int, float]] = {
            "ErrorEquals": self.error_equals
        }
        if interval_seconds := self.interval_seconds:  # pragma: no cover
            compiled["IntervalSeconds"] = interval_seconds
        if backoff_rate := self.backoff_rate:  # pragma: no cover
            compiled["BackoffRate"] = backoff_rate
        if (max_attempts := self.max_attempts) is not None:  # pragma: no cover
            compiled["MaxAttempts"] = max_attempts
        return compiled


@dataclass
class Catcher:
    """Used to go from an errored state to another state."""

    error_equals: List[str]
    next_state: AbstractState

    def compile(self) -> Dict[str, Union[List[str], str]]:  # noqa: A003
        """Compile the Catcher to Amazon States Language.

        Returns:
            A Catcher in Amazon States Language.
        """
        compiled: Dict[str, Union[List[str], str]] = {"ErrorEquals": self.error_equals}
        compiled["Next"] = self.next_state.name
        return compiled


class AbstractRetryCatchState(AbstractResultSelectorState):
    """An Amazon States Language state including Retry and Catch."""

    def __init__(self, *args: Any, **kwargs: Any):
        """Initialize subclasses.

        Args:
            args: Args to pass to parent classes.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self._retriers: List[Retrier] = []
        self._catchers: List[Catcher] = []

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if retriers := self._retriers:
            compiled["Retry"] = [retrier.compile() for retrier in retriers]
        if catchers := self._catchers:
            compiled["Catch"] = [catcher.compile() for catcher in catchers]
        return compiled

    def add_retrier(
        self,
        error_equals: List[str],
        *,
        interval_seconds: Optional[int] = None,
        backoff_rate: Optional[float] = None,
        max_attempts: Optional[int] = None,
    ) -> AbstractRetryCatchState:
        """Add a Retrier to the state.

        Retry the state by specifying a list of Retriers that describes the
        retry policy for different errors.

        Args:
            error_equals: A list of error names.
            interval_seconds: The number of seconds before the first retry
                attempt. Defaults to 1 if not specified.
            backoff_rate: A number which is the multiplier that increases the
                retry interval on each attempt. Defaults to 2.0 if not specified.
            max_attempts: The maximum number of retry to attempt. Defaults to 3
                if not specified. A value of zero means that the error should never
                be retried.

        Returns:
            Itself.
        """
        retrier = Retrier(
            error_equals=error_equals,
            interval_seconds=interval_seconds,
            backoff_rate=backoff_rate,
            max_attempts=max_attempts,
        )
        self._retriers.append(retrier)
        return self

    def add_catcher(
        self,
        error_equals: List[str],
        *,
        next_state: AbstractState,
    ) -> AbstractRetryCatchState:
        """Add a Catcher to the state.

        When a state reports an error and either there is no Retrier, or retries
        have failed to resolve the error, the interpreter will try to find a
        relevant Catcher which determines which state to transition to.

        Args:
            error_equals: A list of error names.
            next_state: The name of the next state.

        Returns:
            Itself.
        """
        catcher = Catcher(
            error_equals=error_equals,
            next_state=next_state,
        )
        self._catchers.append(catcher)
        return self


class TaskState(AbstractRetryCatchState):
    """The Task State executes the work identified by the Resource field.

    >>> task_state = TaskState("Task", resource="123").add_retrier(["SomeError"], max_attempts=0)
    >>> task_state.compile()
    {'Type': 'Task', 'End': True, 'Retry': [{'ErrorEquals': ['SomeError'], 'MaxAttempts': 0}], 'Resource': '123'}

    >>> fail_state = FailState("Fail", error="SomeError", cause="I did it!")
    >>> _ = task_state >> fail_state

    When the state machine simulates the previous example, task_state should not
    get retried as even though a retrier was set for the thrown error, max
    attempts set to zero means it will not be retried.

    >>> transition_state = TaskState("Cleanup", resource="456")
    >>> _ = task_state.add_catcher(["States.ALL"], next_state=transition_state)
    >>> task_state.compile()
    {'Type': 'Task', 'Next': 'Fail', 'Retry': [{'ErrorEquals': ['SomeError'], 'MaxAttempts': 0}], 'Catch': [{'ErrorEquals': ['States.ALL'], 'Next': 'Cleanup'}], 'Resource': '123'}

    >>> another_fail_state = FailState("AnotherFail", error="AnotherError", cause="I did it again!")
    >>> _ = task_state >> another_fail_state

    When the state machine simulates the previous example, in this case, we
    should end up at `transition_state` because "States.ALL" catches all errors
    and transitions to `transition_state`.
    """

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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the Task State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state from running the mock function.
        """
        return resource_to_mock_fn[self.resource](state_input)


class ParallelState(AbstractRetryCatchState):
    """The Parallel State causes parallel execution of branches."""

    state_type = "Parallel"


class MapState(AbstractRetryCatchState):
    """The Map State processes all the elements of an array."""

    state_type = "Map"
