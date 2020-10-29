import contextlib
from contextlib import redirect_stdout
from io import StringIO

import pytest

from awsstepfuncs import FailState, MapState, StateMachine, TaskState


@pytest.fixture()
def state_input():
    return {
        "ship-date": "2016-03-14T01:59:00Z",
        "detail": {
            "delivery-partner": "UQS",
            "shipped": [
                {"prod": "R31", "dest-code": 9511, "quantity": 1344},
                {"prod": "S39", "dest-code": 9511, "quantity": 40},
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
        stdout = [line for line in fp.getvalue().split("\n") if line]

    expected_state_output = [
        {"prod": "R31", "dest-code": 9511, "quantity": 2688},
        {"prod": "S39", "dest-code": 9511, "quantity": 80},
    ]
    assert stdout == [
        "Starting simulation of state machine",
        "Running MapState('Validate-All')",
        f"State input: {state_input}",
        f'State input after applying input path of "$.detail": {state_input["detail"]}',
        "Starting simulation of state machine",
        "Running TaskState('Validate')",
        f"State input: {state_input['detail']['shipped'][0]}",
        f'State input after applying input path of "$": {state_input["detail"]["shipped"][0]}',
        f'Output from applying result path of "$": {expected_state_output[0]}',
        f'State output after applying output path of "$": {expected_state_output[0]}',
        f"State output: {expected_state_output[0]}",
        "Terminating simulation of state machine",
        "Starting simulation of state machine",
        "Running TaskState('Validate')",
        f"State input: {state_input['detail']['shipped'][1]}",
        f'State input after applying input path of "$": {state_input["detail"]["shipped"][1]}',
        f'Output from applying result path of "$": {expected_state_output[1]}',
        f'State output after applying output path of "$": {expected_state_output[1]}',
        f"State output: {expected_state_output[1]}",
        "Terminating simulation of state machine",
        f'Output from applying result path of "$": {expected_state_output}',
        f'State output after applying output path of "$": {expected_state_output}',
        f"State output: {expected_state_output}",
        "Terminating simulation of state machine",
    ]
    assert state_output == expected_state_output


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
        "Running MapState('Validate-All')",
        f"State input: {state_input}",
        f'State input after applying input path of "$.detail": {state_input["detail"]}',
        "Error encountered in state, checking for catchers",
        "Found catcher, transitioning to FailState('Fail', error='ValueError', cause='Me')",
        "State output: {}",
        "Running FailState('Fail', error='ValueError', cause='Me')",
        "State input: {}",
        "State output: {}",
        "Terminating simulation of state machine",
    ]
