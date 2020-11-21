import pytest

from awsstepfuncs import MapState, StateMachine, TaskState


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


@pytest.fixture()
def resource():
    return "<arn>"


@pytest.fixture()
def iterator(resource):
    task_state = TaskState("Validate", resource=resource)
    return StateMachine(start_state=task_state)


@pytest.fixture()
def mock_fn():
    def _mock_fn(event, context):
        event["quantity"] *= 2
        return event

    return _mock_fn


def test_map_state_foo(resource, iterator, state_input, capture_stdout, mock_fn):
    map_state = MapState(
        "Validate-All",
        input_path="$.detail",
        items_path="$.shipped",
        max_concurrency=0,
        iterator=iterator,
    )
    state_machine = StateMachine(start_state=map_state)

    stdout = capture_stdout(
        lambda: state_machine.simulate(
            state_input,
            resource_to_mock_fn={resource: mock_fn},
        )
    )

    assert (
        stdout
        == """Starting simulation of state machine
Executing MapState('Validate-All')
State input: {'ship-date': '2016-03-14T01:59:00Z', 'detail': {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]}}
State input after applying input path of $.detail: {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]}
Items after applying items_path of $.shipped: [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]
Starting simulation of state machine
Executing TaskState('Validate')
State input: {'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}
State input after applying input path of $: {'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}
Output from applying result path of $: {'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}
State output after applying output path of $: {'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}
State output: {'prod': 'R31', 'dest-code': 9511, 'quantity': 2688}
Terminating simulation of state machine
Starting simulation of state machine
Executing TaskState('Validate')
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
"""
    )

    assert state_machine.compile() == {
        "StartAt": "Validate-All",
        "States": {
            "Validate-All": {
                "Type": "Map",
                "InputPath": "$.detail",
                "End": True,
                "ItemsPath": "$.shipped",
                "MaxConcurrency": 0,
                "Iterator": {
                    "StartAt": "Validate",
                    "States": {
                        "Validate": {"Type": "Task", "End": True, "Resource": "<arn>"}
                    },
                },
            }
        },
    }


def test_map_state_bad_items_path(
    resource, iterator, state_input, capture_stdout, mock_fn
):
    map_state = MapState(
        "Validate-All",
        input_path="$.detail",
        items_path="$.delivery-partner",
        max_concurrency=0,
        iterator=iterator,
    )
    state_machine = StateMachine(start_state=map_state)

    stdout = capture_stdout(
        lambda: state_machine.simulate(
            state_input,
            resource_to_mock_fn={resource: mock_fn},
        )
    )

    assert (
        stdout
        == """Starting simulation of state machine
Executing MapState('Validate-All')
State input: {'ship-date': '2016-03-14T01:59:00Z', 'detail': {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]}}
State input after applying input path of $.detail: {'delivery-partner': 'UQS', 'shipped': [{'prod': 'R31', 'dest-code': 9511, 'quantity': 1344}, {'prod': 'S39', 'dest-code': 9511, 'quantity': 40}]}
Items after applying items_path of $.delivery-partner: UQS
StateSimulationError encountered in state
Checking for catchers
No catchers were matched
State output: {}
Terminating simulation of state machine
"""
    )
