"""State definitions.

Class structure based on this table: https://states-language.net/spec.html#state-type-table

Each row in the table is its own ABC. All latter rows inherit from previous
rows. States (the columns) are concrete classes that inherit from the ABC that
has the right fields available.

Each concrete class should implement its own _execute() method that will Execute the
state according to its business logic when running a simulation. Each concrete
class should also define a constant class variable called `state_type` that
corresponds to type in Amazon States Language.

There are two interesting methods common for many classes:
- `simulate()`: Simulate the state including input/output processing
- `_execute()`: Execute the state, eg. for a Wait State, wait the designated time
"""
from __future__ import annotations

import time
from abc import ABC
from datetime import datetime
from typing import Any, Dict, List, Optional

import dateutil.parser
import pause

from awsstepfuncs.abstract_state import (
    AbstractInputPathOutputPathState,
    AbstractNextOrEndState,
    AbstractParametersState,
    AbstractRetryCatchState,
    AbstractState,
)
from awsstepfuncs.choice import AbstractChoice
from awsstepfuncs.errors import (
    AWSStepFuncsValueError,
    FailStateError,
    NoChoiceMatchedError,
    StateSimulationError,
    TaskFailedError,
)
from awsstepfuncs.printer import Color, Style
from awsstepfuncs.reference_path import ReferencePath
from awsstepfuncs.state_machine import StateMachine
from awsstepfuncs.types import ResourceToMockFn

MAX_STATE_NAME_LENGTH = 128


class TerminalStateMixin(ABC):
    """A mixin for blocking rshift for terminal states."""

    def __rshift__(self, _: AbstractState, /) -> AbstractState:
        """Overload >> operator to set state execution order.

        You cannot set a next state on a ChoiceState, SucceedState, or FailState
        as they are terminal states.

        Args:
            _: The other state besides self.

        Raises:
            AWSStepFuncsValueError: Raised when trying to set next state on a terminal
                state.
        """
        raise AWSStepFuncsValueError(
            f"{self.__class__.__name__} cannot have a next state"
        )


class FailState(TerminalStateMixin, AbstractState):
    """The Fail State terminates the machine and marks it as a failure."""

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

    def _execute(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Execute the Fail State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            FailStateError: Always raised with the error and cause.
        """
        raise FailStateError(error=self.error, cause=self.cause)

    def __str__(self) -> str:
        """Create a human-readable string representation of a state.

        Returns:
            Human-readable string representation of a state.
        """
        return f"{self.__class__.__name__}({self.name!r}, error={self.error!r}, cause={self.cause!r})"


class SucceedState(TerminalStateMixin, AbstractInputPathOutputPathState):
    """The Succeed State terminates with a mark of success.

    The Succeed State's output is the same as its input.
    """

    state_type = "Succeed"

    def _execute(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Execute the Succeed State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state, the same as its input.
        """
        return state_input


class ChoiceState(TerminalStateMixin, AbstractInputPathOutputPathState):
    """A Choice State adds branching logic to a state machine."""

    state_type = "Choice"

    def __init__(
        self,
        *args: Any,
        choices: List[AbstractChoice],
        default: Optional[AbstractState] = None,
        **kwargs: Any,
    ):
        """Initialize a Choice State.

        Args:
            args: Args to pass to parent classes.
            choices: The branches of the Choice State.
            default: The default state to transition to if none of the choices
                evaluate to true.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.choices = choices
        self.default = default

    def compile(self) -> Dict[str, Any]:  # noqa: A003 pragma: no cover
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        # TODO
        compiled = super().compile()
        compiled.pop("End")  # Not correct for Choice State
        return compiled  # pragma: no cover

    def _execute(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Execute the Choice State.

        Sets the next state.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            NoChoiceMatchedError: Raised when no choice is true and no default is set.

        Returns:
            The output of the state.
        """
        for choice in self.choices:
            if choice.evaluate(state_input):
                self.next_state = choice.next_state
                return state_input
        else:
            self.print("No choice evaluated to true", style=Style.DIM)
            if self.default:
                self.print(
                    "Choosing next state by the default set",
                    color=Color.GREEN,
                    emoji="➡️",
                )
                self.next_state = self.default
            else:
                raise NoChoiceMatchedError("No choice is true and no default set")


class WaitState(AbstractNextOrEndState):
    """A Wait State causes the interpreter to delay the machine for a specified time.

    You can specify a timestamp to wait until. If the time has already past,
    then there is no wait.

    Alternatively, you can use state input to specify the number of seconds wait
    by specifying a Reference Path `seconds_path`.

    Similarily, you can use state input to specify the timestamp (in ISO 8601
    format) to wait until.

    Exactly one must be defined: `seconds`, `timestamp`, `seconds_path`,
    `timestamp_path`.

    Refs: https://states-language.net/#wait-state
    """

    state_type = "Wait"

    def __init__(
        self,
        *args: Any,
        seconds: Optional[int] = None,
        timestamp: Optional[datetime] = None,
        seconds_path: Optional[str] = None,
        timestamp_path: Optional[str] = None,
        **kwargs: Any,
    ):
        """Initialize a Wait State.

        Args:
            args: Args to pass to parent classes.
            seconds: The number of seconds to wait.
            timestamp: Wait until the specified time.
            seconds_path: A Reference Path to the number of seconds to wait.
            timestamp_path: A Reference Path to the timestamp to wait until.
            kwargs: Kwargs to pass to parent classes.

        Raises:
            AWSStepFuncsValueError: Raised when not exactly one is defined: seconds,
                timestamp, seconds_path, timestamp_path.
        """
        super().__init__(*args, **kwargs)

        if (
            sum(
                bool(variable)
                for variable in [seconds, timestamp, seconds_path, timestamp_path]
            )
            != 1
        ):
            raise AWSStepFuncsValueError(
                "Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path"
            )

        if seconds and not (seconds > 0):
            raise AWSStepFuncsValueError("seconds must be greater than zero")

        self.seconds = seconds
        self.timestamp = timestamp
        self.seconds_path = ReferencePath(seconds_path) if seconds_path else None
        self.timestamp_path = ReferencePath(timestamp_path) if timestamp_path else None

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        >>> WaitState("Wait!", seconds=5).compile()
        {'Type': 'Wait', 'End': True, 'Seconds': 5}

        >>> WaitState("Wait!", timestamp=datetime(2020,1,1)).compile()
        {'Type': 'Wait', 'End': True, 'Timestamp': '2020-01-01T00:00:00'}

        >>> WaitState("Wait!", seconds_path="$.numSeconds").compile()
        {'Type': 'Wait', 'End': True, 'SecondsPath': '$.numSeconds'}

        >>> WaitState("Wait!", timestamp_path="$.meta.timeToWait").compile()
        {'Type': 'Wait', 'End': True, 'TimestampPath': '$.meta.timeToWait'}

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        if seconds := self.seconds:
            compiled["Seconds"] = seconds
        if timestamp := self.timestamp:
            compiled["Timestamp"] = timestamp.isoformat()
        if (seconds_path := self.seconds_path) is not None:
            compiled["SecondsPath"] = str(seconds_path)
        if (timestamp_path := self.timestamp_path) is not None:
            compiled["TimestampPath"] = str(timestamp_path)
        return compiled

    def __str__(self) -> str:
        """Create a human-readable string representation of a state.

        Returns:
            Human-readable string representation of a state.
        """
        output = f"{self.__class__.__name__}({self.name!r}"
        if seconds := self.seconds:
            output += f", seconds={seconds!r}"
        if timestamp := self.timestamp:
            output += f", timestamp={timestamp.isoformat()!r}"
        if seconds_path := self.seconds_path:
            output += f", seconds_path={seconds_path!r}"
        if timestamp_path := self.timestamp_path:
            output += f", timestamp_path={timestamp_path!r}"
        return output + ")"

    def _execute(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Execute the Wait State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            StateSimulationError: Raised when seconds_path doesn't point to an integer.

        Returns:
            The output of the state, same as input for the Wait State.
        """
        if seconds := self.seconds:
            self._wait_seconds(seconds)

        elif self.timestamp and (datetime.now() < self.timestamp):
            self._wait_for_timestamp(self.timestamp)

        elif (seconds_path := self.seconds_path) is not None:
            seconds = seconds_path.apply(state_input)
            if not isinstance(seconds, int):
                raise StateSimulationError("seconds_path should point to an integer")
            self._wait_seconds(seconds)

        elif (timestamp_path := self.timestamp_path) is not None:
            timestamp = timestamp_path.apply(state_input)
            dt = dateutil.parser.parse(timestamp)
            self._wait_for_timestamp(dt)

        return state_input

    def _wait_seconds(self, seconds: int) -> None:
        """Wait for the specified number of seconds."""
        self.print(f"Waiting {seconds} seconds", style=Style.DIM)
        time.sleep(seconds)

    def _wait_for_timestamp(self, timestamp: datetime) -> None:
        self.print(f"Waiting until {timestamp.isoformat()}", style=Style.DIM)
        pause.until(timestamp)


class PassState(AbstractParametersState):
    """The Pass State by default passes its input to its output, performing no work.

    If `result` is passed, its value is treated as the output of a virtual task.

    If `result_path` is specified, the `result` will be placed on that Reference
    Path.
    """

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

    def _execute(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Execute the Pass State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Returns:
            The output of the state, same as input if result is not provided.
        """
        if result := self.result:
            return result
        else:
            return state_input


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

    def _execute(
        self,
        state_input: Any,
        resource_to_mock_fn: ResourceToMockFn,
    ) -> Any:
        """Execute the Task State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            TaskFailedError: Raised if there is an exception when executing the
                mock function.

        Returns:
            The output of the state from executing the mock function given the
            state's input.
        """
        # TODO: Add mock context?
        mock_fn = resource_to_mock_fn[self.resource]
        try:
            state_output = mock_fn(state_input, context=None)
        except Exception as exc:
            raise TaskFailedError(str(exc))
        else:
            return state_output


class ParallelState(AbstractRetryCatchState):
    """The Parallel State causes parallel execution of branches."""

    state_type = "Parallel"


class MapState(AbstractRetryCatchState):
    """The Map State processes all the elements of an array."""

    state_type = "Map"

    def __init__(
        self,
        *args: Any,
        iterator: StateMachine,
        items_path: str = "$",
        max_concurrency: int,
        **kwargs: Any,
    ):
        """Initialize a Map State.

        Args:
            args: Args to pass to parent classes.
            iterator: The state machine which will process each element of the
                array.
            items_path: A Reference Path identifying where in the effective
                input the array field is found.
            max_concurrency: The upper bound on how many invocations of the
                Iterator may run in parallel.
            kwargs: Kwargs to pass to parent classes.
        """
        super().__init__(*args, **kwargs)
        self.iterator = iterator
        self.items_path = items_path
        self.max_concurrency = max_concurrency

    def compile(self) -> Dict[str, Any]:  # noqa: A003
        """Compile the state to Amazon States Language.

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        compiled["ItemsPath"] = self.items_path
        compiled["MaxConcurrency"] = self.max_concurrency
        compiled["Iterator"] = self.iterator.compile()
        return compiled

    def _execute(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Execute the Map State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            StateSimulationError: Raised when items_path does not evaluate to a
                list.

        Returns:
            The output of the state by running the iterator state machine for
            all items.
        """
        items = ReferencePath(self.items_path).apply(state_input)
        self.print(
            f"Items after applying items_path of {self.items_path}: {items}",
            style=Style.DIM,
        )
        if not isinstance(items, list):
            raise StateSimulationError("items_path must yield a list")

        state_output = []
        for item in items:
            state_output.append(
                self.iterator.simulate(item, resource_to_mock_fn=resource_to_mock_fn)
            )
        return state_output
