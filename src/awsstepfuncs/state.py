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

    >>> fail_state = FailState("Failure", error="IFailed", cause="I failed!")
    >>> state_machine = StateMachine(start_state=fail_state)
    >>> state_output = state_machine.simulate()
    Starting simulation of state machine
    Running FailState('Failure', error='IFailed', cause='I failed!')
    State input: {}
    State output: {}
    Terminating simulation of state machine
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

        >>> fail_state = FailState("FailState", error="ErrorA", cause="Kaiju attack")
        >>> fail_state.compile()
        {'Type': 'Fail', 'Error': 'ErrorA', 'Cause': 'Kaiju attack'}

        Returns:
            A dictionary representing the compiled state in Amazon States
            Language.
        """
        compiled = super().compile()
        compiled["Error"] = self.error
        compiled["Cause"] = self.cause
        return compiled

    def __str__(self) -> str:
        """Create a human-readable string representation of a state.

        Returns:
            Human-readable string representation of a state.
        """
        return f"{self.__class__.__name__}({self.name!r}, error={self.error!r}, cause={self.cause!r})"


class SucceedState(TerminalStateMixin, AbstractInputPathOutputPathState):
    """The Succeed State terminates with a mark of success.

    The branch can either be:
        - The entire state machine
        - A branch of a Parallel State
        - An iteration of a Map State
    """

    state_type = "Succeed"


class ChoiceState(TerminalStateMixin, AbstractInputPathOutputPathState):
    """A Choice State adds branching logic to a state machine.

    Define some states that can be conditionally transitioned to by the
    Choice State.

    >>> from awsstepfuncs import *
    >>> public_state = PassState("Public")
    >>> value_in_twenties_state = PassState("ValueInTwenties")
    >>> start_audit_state = PassState("StartAudit")
    >>> record_event_state = PassState("RecordEvent")

    Now we can define a Choice State with branching logic based on
    conditions.

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
    >>> _ = state_machine.simulate(state_input={"type": "Private", "value": 22})
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
    Terminating simulation of state machine

    If no choice evaluates to true, then the default will be chosen.

    >>> _ = state_machine.simulate(state_input={
    ...     "type": "Private",
    ...     "value": 102,
    ...     "auditThreshold": 150,
    ... })
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

    If no choice evaluates to true and no default is set, then there will be an
    error.

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
    >>> _ = state_machine.simulate(state_input={
    ...     "type": "Private",
    ...     "value": 102,
    ...     "auditThreshold": 150,
    ... })
    Starting simulation of state machine
    Running ChoiceState('DispatchEvent')
    State input: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
    State input after applying input path of $: {'type': 'Private', 'value': 102, 'auditThreshold': 150}
    No choice evaluated to true
    Error encountered in state, checking for catchers
    State output: {}
    Terminating simulation of state machine
    """

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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the Choice State.

        Sets the next state.

        Args:
            state_input: The input state data.
            resource_to_mock_fn: A mapping of resource URIs to mock functions to
                use if the state performs a task.

        Raises:
            ValueError: Raised when no choice is true and no default is set.

        Returns:
            The output of the state.
        """
        for choice in self.choices:
            if choice.evaluate(state_input):
                self.next_state = choice.next_state
                return state_input
        else:
            print("No choice evaluated to true")
            if self.default:
                print("Choosing next state by the default set")
                self.next_state = self.default
            else:
                raise ValueError("No choice is true and no default set")


class WaitState(AbstractNextOrEndState):
    """A Wait State causes the interpreter to delay the machine for a specified time.

    You can specify the number of seconds to wait.

    >>> wait_state = WaitState("Wait!", seconds=1)
    >>> state_machine = StateMachine(start_state=wait_state)
    >>> state_output = state_machine.simulate()
    Starting simulation of state machine
    Running WaitState('Wait!', seconds=1)
    State input: {}
    State input after applying input path of $: {}
    Waiting 1 seconds
    State output after applying output path of $: {}
    State output: {}
    Terminating simulation of state machine

    Seconds must be an integer greater than zero.

    >>> WaitState("Wait!", seconds=-1)
    Traceback (most recent call last):
        ...
    ValueError: seconds must be greater than zero

    You can specify a timestamp to wait until. If the time has already past,
    then there is no wait.

    >>> from datetime import datetime, timedelta
    >>> wait_state = WaitState("Wait!", timestamp=datetime(2020, 1, 1))
    >>> state_machine = StateMachine(start_state=wait_state)
    >>> state_output = state_machine.simulate()
    Starting simulation of state machine
    Running WaitState('Wait!', timestamp='2020-01-01T00:00:00')
    State input: {}
    State input after applying input path of $: {}
    State output after applying output path of $: {}
    State output: {}
    Terminating simulation of state machine

    Alternatively, you can use state input to specify the number of seconds wait
    by specifying a Reference Path `seconds_path`.

    >>> wait_state = WaitState("Wait!", seconds_path="$.numSeconds")
    >>> state_machine = StateMachine(start_state=wait_state)
    >>> state_output = state_machine.simulate(state_input={"numSeconds": 1})
    Starting simulation of state machine
    Running WaitState('Wait!', seconds_path='$.numSeconds')
    State input: {'numSeconds': 1}
    State input after applying input path of $: {'numSeconds': 1}
    Waiting 1 seconds
    State output after applying output path of $: {'numSeconds': 1}
    State output: {'numSeconds': 1}
    Terminating simulation of state machine

    A `ValueError` will be thrown if `seconds_path` isn't a reference path to an
    integer. This is considered a runtime exception and will be treated as an
    error during the simulation.

    >>> wait_state = WaitState("Wait!", seconds_path="$.numSeconds")
    >>> state_machine = StateMachine(start_state=wait_state)
    >>> state_output = state_machine.simulate(state_input={"numSeconds": "hello"})
    Starting simulation of state machine
    Running WaitState('Wait!', seconds_path='$.numSeconds')
    State input: {'numSeconds': 'hello'}
    State input after applying input path of $: {'numSeconds': 'hello'}
    Error encountered in state, checking for catchers
    State output: {}
    Terminating simulation of state machine

    Similarily, you can use state input to specify the timestamp (in ISO 8601
    format) to wait until.

    >>> wait_state = WaitState("Wait!", timestamp_path="$.meta.timeToWait")
    >>> state_machine = StateMachine(start_state=wait_state)
    >>> state_output = state_machine.simulate(state_input={"meta": {"timeToWait": "2020-01-01T00:00:00"}})
    Starting simulation of state machine
    Running WaitState('Wait!', timestamp_path='$.meta.timeToWait')
    State input: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
    State input after applying input path of $: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
    Waiting until 2020-01-01T00:00:00
    State output after applying output path of $: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
    State output: {'meta': {'timeToWait': '2020-01-01T00:00:00'}}
    Terminating simulation of state machine

    Exactly one must be defined: `seconds`, `timestamp`, `seconds_path`,
    `timestamp_path`.

    Multiple parameters set:

    >>> WaitState("Wait", seconds=5, timestamp=datetime.now())
    Traceback (most recent call last):
        ...
    ValueError: Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path

    No parameters set:

    >>> WaitState("Wait")
    Traceback (most recent call last):
        ...
    ValueError: Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path

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
            ValueError: Raised when not exactly one is defined: seconds,
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
            raise ValueError(
                "Exactly one must be defined: seconds, timestamp, seconds_path, timestamp_path"
            )

        if seconds and not (seconds > 0):
            raise ValueError("seconds must be greater than zero")

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

    def _run(self, state_input: Any, resource_to_mock_fn: ResourceToMockFn) -> Any:
        """Run the Wait State.

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
            if not isinstance(seconds, int):
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
    """The Pass State by default passes its input to its output, performing no work.

    >>> pass_state1 = PassState("Pass 1", comment="The starting state")
    >>> pass_state2 = PassState("Pass 2")
    >>> pass_state3 = PassState("Pass 3")

    Define the state machine.

    >>> _ = pass_state1 >> pass_state2 >> pass_state3
    >>> state_machine = StateMachine(start_state=pass_state1)

    Make sure that the workflow is correctly specified.

    >>> [state.name for state in state_machine.start_state]
    ['Pass 1', 'Pass 2', 'Pass 3']

    Check that it compiles correctly.

    >>> compiled = state_machine.compile()
    >>> expected = {
    ...     "StartAt": "Pass 1",
    ...     "States": {
    ...         "Pass 2": {"Type": "Pass", "Next": "Pass 3"},
    ...         "Pass 1": {"Type": "Pass", "Comment": "The starting state", "Next": "Pass 2"},
    ...         "Pass 3": {"Type": "Pass", "End": True},
    ...     },
    ... }
    >>> assert compiled == expected

    Then you can run a simulation to debug it.

    >>> _ = state_machine.simulate()
    Starting simulation of state machine
    Running PassState('Pass 1')
    State input: {}
    State input after applying input path of $: {}
    Output from applying result path of $: {}
    State output after applying output path of $: {}
    State output: {}
    Running PassState('Pass 2')
    State input: {}
    State input after applying input path of $: {}
    Output from applying result path of $: {}
    State output after applying output path of $: {}
    State output: {}
    Running PassState('Pass 3')
    State input: {}
    State input after applying input path of $: {}
    Output from applying result path of $: {}
    State output after applying output path of $: {}
    State output: {}
    Terminating simulation of state machine

    If `result` is passed, its value is treated as the output of a virtual task.

    >>> result = {"Hello": "world!"}
    >>> pass_state = PassState("Passing", result=result)
    >>> state_machine = StateMachine(start_state=pass_state)
    >>> state_output = state_machine.simulate()
    Starting simulation of state machine
    Running PassState('Passing')
    State input: {}
    State input after applying input path of $: {}
    Output from applying result path of $: {'Hello': 'world!'}
    State output after applying output path of $: {'Hello': 'world!'}
    State output: {'Hello': 'world!'}
    Terminating simulation of state machine
    >>> assert state_output == result

    If `result_path` is specified, the `result` will be placed on that Reference
    Path.

    >>> result = {"Hello": "world!"}
    >>> pass_state = PassState("Passing", result=result, result_path="$.result")
    >>> state_machine = StateMachine(start_state=pass_state)
    >>> _ = state_machine.simulate(state_input={"sum": 42})
    Starting simulation of state machine
    Running PassState('Passing')
    State input: {'sum': 42}
    State input after applying input path of $: {'sum': 42}
    Output from applying result path of $.result: {'sum': 42, 'result': {'Hello': 'world!'}}
    State output after applying output path of $: {'sum': 42, 'result': {'Hello': 'world!'}}
    State output: {'sum': 42, 'result': {'Hello': 'world!'}}
    Terminating simulation of state machine

    Be careful! The state name has a maximum length of 128 characters.

    >>> PassState("a" * 129)
    Traceback (most recent call last):
        ...
    ValueError: State name cannot exceed 128 characters
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

        >>> result = {"Hello": "world!"}
        >>> pass_state = PassState("Passing", result=result)
        >>> pass_state.compile()
        {'Type': 'Pass', 'End': True, 'Result': {'Hello': 'world!'}}

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

        >>> task_state = TaskState("Task", resource="arn:aws:lambda:ap-southeast-2:710187714096:function:DummyResource")
        >>> task_state.compile()
        {'Type': 'Task', 'End': True, 'Resource': 'arn:aws:lambda:ap-southeast-2:710187714096:function:DummyResource'}

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
    """The Map State processes all the elements of an array.

    >>> resource = "<arn>"
    >>> task_state = TaskState("Validate", resource=resource)
    >>> iterator = StateMachine(start_state=task_state)
    >>> map_state = MapState(
    ...     "Validate-All",
    ...     input_path="$.detail",
    ...     items_path="$.shipped",
    ...     max_concurrency=0,
    ...     iterator=iterator,
    ... )
    >>> state_machine = StateMachine(start_state=map_state)

    You can simulate a state machine with a Map State.

    >>> state_input = {
    ...    "ship-date": "2016-03-14T01:59:00Z",
    ...    "detail": {
    ...        "delivery-partner": "UQS",
    ...        "shipped": [
    ...            {"prod": "R31", "dest-code": 9511, "quantity": 1344},
    ...            {"prod": "S39", "dest-code": 9511, "quantity": 40},
    ...        ],
    ...    },
    ... }
    >>> def mock_fn(state_input):
    ...     state_input["quantity"] *= 2
    ...     return state_input
    >>> _ = state_machine.simulate(
    ...     state_input=state_input,
    ...     resource_to_mock_fn={resource: mock_fn},
    ... )
    Starting simulation of state machine
    Running MapState('Validate-All')
    State input: {'ship-date': '2016-03-14T01:59:00Z', 'detail': {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]}}
    State input after applying input path of $.detail: {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]}
    Items after applying items_path of $.shipped: [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]
    Starting simulation of state machine
    Running TaskState('Validate')
    State input: {'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}
    State input after applying input path of $: {'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}
    Output from applying result path of $: {'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}
    State output after applying output path of $: {'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}
    State output: {'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}
    Terminating simulation of state machine
    Starting simulation of state machine
    Running TaskState('Validate')
    State input: {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}
    State input after applying input path of $: {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}
    Output from applying result path of $: {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}
    State output after applying output path of $: {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}
    State output: {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}
    Terminating simulation of state machine
    Output from applying result path of $: [{'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}]
    State output after applying output path of $: [{'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}]
    State output: [{'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}]
    Terminating simulation of state machine

    You can also compile a state machine with a Map State.

    >>> output = state_machine.compile()
    >>> expected_output = {
    ...     "StartAt": "Validate-All",
    ...     "States": {
    ...         "Validate-All": {
    ...             "Type": "Map",
    ...             "InputPath": "$.detail",
    ...             "End": True,
    ...             "ItemsPath": "$.shipped",
    ...             "MaxConcurrency": 0,
    ...             "Iterator": {
    ...                 "StartAt": "Validate",
    ...                 "States": {
    ...                     "Validate": {"Type": "Task", "End": True, "Resource": "<arn>"}
    ...                 },
    ...             },
    ...         }
    ...     },
    ... }
    >>> assert output == expected_output

    Be careful that `items_path` Reference Path actually yields a list.

    >>> map_state = MapState(
    ...     "Validate-All",
    ...     input_path="$.detail",
    ...     items_path="$.delivery-partner",
    ...     max_concurrency=0,
    ...     iterator=iterator,
    ... )
    >>> state_machine = StateMachine(start_state=map_state)
    >>> _ = state_machine.simulate(
    ...     state_input=state_input,
    ...     resource_to_mock_fn={resource: mock_fn},
    ... )
    Starting simulation of state machine
    Running MapState('Validate-All')
    State input: {'ship-date': '2016-03-14T01:59:00Z', 'detail': {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}]}}
    State input after applying input path of $.detail: {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 80}]}
    Items after applying items_path of $.delivery-partner: UQS
    Error encountered in state, checking for catchers
    No catchers were matched
    State output: {}
    Terminating simulation of state machine
    """

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
        items = ReferencePath(self.items_path).apply(state_input)
        print(f"Items after applying items_path of {self.items_path}: {items}")
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
