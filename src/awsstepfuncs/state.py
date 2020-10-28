"""State definitions.

Class structure based on this table: https://states-language.net/spec.html#state-type-table

Each row in the table is its own ABC. All latter rows inherit from previous
rows. States (the columns) are concrete classes that inherit from the ABC that
has the right fields available.

Each concrete class should implement its own _run() method that will run the
state according to its business logic when running a simulation. Each concrete
class should also define a constant class variable called `state_type` that
corresponds to type in Amazon States Language.

There are two interesting methods common for many classes:
- simulate() -- Simulate the state including input/output processing
- _run() -- Run the state, eg. for a WaitState wait the designated time
"""
from __future__ import annotations

import time
from abc import ABC
from datetime import datetime
from typing import Any, Dict, Optional

import dateutil.parser
import pause

from awsstepfuncs.abstract_state import (
    AbstractInputPathOutputPathState,
    AbstractNextOrEndState,
    AbstractParametersState,
    AbstractRetryCatchState,
    AbstractState,
)
from awsstepfuncs.json_path import JSONPath
from awsstepfuncs.state_machine import StateMachine
from awsstepfuncs.types import ResourceToMockFn

MAX_STATE_NAME_LENGTH = 128


class TerminalStateMixin(ABC):
    """A mixin for blocking rshift for terminal states."""

    def __rshift__(self, _: AbstractState, /) -> AbstractState:
        """Overload >> operator to set state execution order.

        You cannot set a next state on a ChoiceState, SucceedState, or FailState
        as they are terminal states.

        >>> fail_state = FailState("Fail", error="JustBecause", cause="Because I feel like it")
        >>> pass_state = PassState("Pass")
        >>> fail_state >> pass_state
        Traceback (most recent call last):
            ...
        ValueError: FailState cannot have a next state

        Args:
            _: The other state besides self.

        Raises:
            ValueError: Raised when trying to set next state on a terminal
                state.
        """
        raise ValueError(f"{self.__class__.__name__} cannot have a next state")


class FailState(TerminalStateMixin, AbstractState):
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


class SucceedState(TerminalStateMixin, AbstractInputPathOutputPathState):
    """The Succeed State terminates with a mark of success.

    The branch can either be:
        - The entire state machine
        - A branch of a Parallel State
        - An iteration of a Map State
    """

    state_type = "Succeed"


class ChoiceState(TerminalStateMixin, AbstractInputPathOutputPathState):
    """A Choice State adds branching logic to a state machine."""

    state_type = "Choice"


class WaitState(AbstractNextOrEndState):
    """A Wait State causes the interpreter to delay the machine for a specified time."""

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

        Either seconds or timestamp must be specified, and they cannot both be
        specified. If timestamp is a datetime that has already passed, then
        there is no wait.

        Refs: https://states-language.net/#wait-state

        Args:
            args: Args to pass to parent classes.
            seconds: The number of seconds to wait.
            timestamp: Wait until the specified time.
            seconds_path: A Reference Path to the number of seconds to wait.
            timestamp_path: A Reference Path to the timestamp to wait until.
            kwargs: Kwargs to pass to parent classes.

        Raises:
            ValueError: Raised when not exactly one is defined: seconds,
                timestamp, seconds_path, timestamp_path.
        """
        super().__init__(*args, **kwargs)

        if (
            sum(bool(a) for a in [seconds, timestamp, seconds_path, timestamp_path])
            != 1
        ):
            raise ValueError(
                "Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path"
            )

        self.seconds = seconds
        self.timestamp = timestamp
        self.seconds_path = JSONPath(seconds_path) if seconds_path else None
        self.timestamp_path = JSONPath(timestamp_path) if timestamp_path else None

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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the Wait State.

        >>> wait_state = WaitState("Wait!", seconds_path="$.numSeconds")
        >>> state_machine = StateMachine(start_state=wait_state)
        >>> state_output = state_machine.simulate(state_input={"numSeconds": 1})
        Starting simulation of state machine
        Running Wait!
        State input: {'numSeconds': 1}
        State input after applying input path of "$": {'numSeconds': 1}
        Waiting 1 seconds
        State output after applying output path of "$": {'numSeconds': 1}
        State output: {'numSeconds': 1}
        Terminating simulation of state machine

        >>> wait_state = WaitState("Wait!", timestamp_path="$.meta.timeToWait")
        >>> state_machine = StateMachine(start_state=wait_state)
        >>> state_output = state_machine.simulate(state_input={"meta": {"timeToWait": "2020-01-01T00:00:00"}})
        Starting simulation of state machine
        Running Wait!
        State input: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
        State input after applying input path of "$": {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
        Waiting until 2020-01-01T00:00:00
        State output after applying output path of "$": {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
        State output: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
        Terminating simulation of state machine

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            ValueError: Raised when seconds_path doesn't point to an integer.

        Returns:
            The output of the state, same as input for the Wait State.
        """
        if seconds := self.seconds:
            self._wait_seconds(seconds)

        elif self.timestamp and (datetime.now() < self.timestamp):
            self._wait_for_timestamp(self.timestamp)

        elif (seconds_path := self.seconds_path) is not None:
            seconds = seconds_path.apply(state_input)
            if not isinstance(seconds, int):  # pragma: no cover
                raise ValueError("seconds_path should point to an integer")
            self._wait_seconds(seconds)

        elif (timestamp_path := self.timestamp_path) is not None:
            timestamp = timestamp_path.apply(state_input)
            dt = dateutil.parser.parse(timestamp)
            self._wait_for_timestamp(dt)

        return state_input

    def _wait_seconds(self, seconds: int) -> None:
        """Wait for the specified number of seconds."""
        print(f"Waiting {seconds} seconds")
        time.sleep(seconds)

    def _wait_for_timestamp(self, timestamp: datetime) -> None:
        print(f"Waiting until {timestamp.isoformat()}")
        pause.until(timestamp)


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
        if result := self.result:
            return result
        else:
            return state_input


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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the Map State.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            ValueError: Raised when ItemsPath does not return a list.

        Returns:
            The output of the state by running the iterator state machine for
            all items.
        """
        items = JSONPath(self.items_path).apply(state_input)
        if not isinstance(items, list):
            raise ValueError("items_path must yield a list")

        state_output = []
        for item in items:
            state_output.append(
                self.iterator.simulate(
                    state_input=item, resource_to_mock_fn=resource_to_mock_fn
                )
            )
        return state_output
