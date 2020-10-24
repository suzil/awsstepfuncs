import contextlib
from contextlib import redirect_stdout
from io import StringIO

import pytest

from awsstepfuncs import MapState, StateMachine, TaskState
from awsstepfuncs.state import FailState


@pytest.fixture()
def state_input():
    return {
        "ship-date": "2016-03-14T01:59:00Z",
        "detail": {
            "delivery-partner": "UQS",
            "shipped": [
                {"prod": "R31", "dest-code": 9511, "quantity": 1344},
                {"prod": "S39", "dest-code": 9511, "quantity": 40},
                {"prod": "R31", "dest-code": 9833, "quantity": 12},
                {"prod": "R40", "dest-code": 9860, "quantity": 887},
                {"prod": "R40", "dest-code": 9511, "quantity": 1220},
            ],
        },
    }


def mock_fn(state_input):
    state_input = state_input.copy()
    state_input["quantity"] *= 2
    return state_input


def test_map_state(state_input):
    resource = "123"
    task_state = TaskState("Validate", resource=resource)
    iterator = StateMachine(start_state=task_state)
    map_state = MapState(
        "Validate-All",
        input_path="$.detail",
        items_path="$.shipped",
        max_concurrency=0,
        iterator=iterator,
    )
    state_machine = StateMachine(start_state=map_state)
    assert state_machine.compile() == {
        "StartAt": "Validate-All",
        "States": {
            "Validate-All": {
                "Type": "Map",
                "InputPath": "$.detail",
                "ItemsPath": "$.shipped",
                "MaxConcurrency": 0,
                "Iterator": {
                    "StartAt": "Validate",
                    "States": {
                        "Validate": {"Type": "Task", "Resource": resource, "End": True}
                    },
                },
                "End": True,
            }
        },
    }

    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate(
                state_input=state_input, resource_to_mock_fn={resource: mock_fn}
            )
        stdout = fp.getvalue()

    assert (
        stdout
        == """Starting simulation of state machine
Running Validate-All
Starting simulation of state machine
Running Validate
Terminating simulation of state machine
Starting simulation of state machine
Running Validate
Terminating simulation of state machine
Starting simulation of state machine
Running Validate
Terminating simulation of state machine
Starting simulation of state machine
Running Validate
Terminating simulation of state machine
Starting simulation of state machine
Running Validate
Terminating simulation of state machine
Terminating simulation of state machine
"""
    )
    assert state_output == [
        {"dest-code": 9511, "prod": "R31", "quantity": 2688},
        {"dest-code": 9511, "prod": "S39", "quantity": 80},
        {"dest-code": 9833, "prod": "R31", "quantity": 24},
        {"dest-code": 9860, "prod": "R40", "quantity": 1774},
        {"dest-code": 9511, "prod": "R40", "quantity": 2440},
    ]


def test_bad_items_path(state_input):
    resource = "123"
    task_state = TaskState("Validate", resource=resource)
    iterator = StateMachine(start_state=task_state)
    fail_state = FailState("Fail", error="ValueError", cause="Me")
    map_state = MapState(
        "Validate-All",
        input_path="$.detail",
        items_path="$.delivery-partner",
        max_concurrency=0,
        iterator=iterator,
    ).add_catcher(["States.ALL"], next_state=fail_state)
    state_machine = StateMachine(start_state=map_state)

    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_machine.simulate(
                state_input=state_input, resource_to_mock_fn={resource: mock_fn}
            )
        stdout = [line for line in fp.getvalue().split("\n") if line]

    assert stdout == [
        "Starting simulation of state machine",
        "Running Validate-All",
        "Running Fail",
        "Terminating simulation of state machine",
    ]
