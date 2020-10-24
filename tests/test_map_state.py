from awsstepfuncs import MapState, StateMachine, TaskState


def test_map_state():
    resource = "123"
    task_state = TaskState("Validate", resource=resource)
    iterator = StateMachine(start_state=task_state)
    map_state = MapState(
        "Validate-All",
        input_path="$.detail",
        items_path="$.shipped",
        max_concurrency=0,
        result_path="$.detail.shipped",
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
                "ResultPath": "$.detail.shipped",
                "End": True,
            }
        },
    }

    state_input = {
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

    def mock_fn(_):
        return "hello"

    # TODO: Implement simulation for map (just returns right now, not actually
    # implemented)
    state_machine.simulate(
        state_input=state_input, resource_to_mock_fn={resource: mock_fn}
    )
