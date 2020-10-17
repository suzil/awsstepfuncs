import contextlib
from contextlib import redirect_stdout
from io import StringIO

import pytest

from awsstepfuncs import PassState, StateMachine, TaskState


@pytest.fixture(scope="session")
def dummy_resource_uri():
    return "arn:aws:lambda:ap-southeast-2:710187714096:function:DivideNumbers"


def test_task_state(compile_state_machine, dummy_resource_uri):
    pass_state = PassState("Pass", comment="The starting state")
    task_state = TaskState("Task", resource_uri=dummy_resource_uri)

    # Define the state machine
    pass_state >> task_state
    state_machine = StateMachine(start_state=pass_state)

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": pass_state.name,
        "States": {
            pass_state.name: {
                "Comment": pass_state.comment,
                "Type": "Pass",
                "Next": task_state.name,
            },
            task_state.name: {
                "Type": "Task",
                "Resource": dummy_resource_uri,
                "End": True,
            },
        },
    }

    # Simulate the state machine

    def mock_fn(data):
        data["foo"] *= 2
        return data

    with contextlib.closing(StringIO()) as fp:
        with redirect_stdout(fp):
            state_output = state_machine.simulate(
                state_input={"foo": 5, "bar": 1},
                resource_to_mock_fn={dummy_resource_uri: mock_fn},
            )
        stdout = fp.getvalue()

    assert state_output == {"foo": 10, "bar": 1}
    assert (
        stdout
        == """Running Pass
Passing
Running Task
"""
    )


def test_result_selector(compile_state_machine, dummy_resource_uri):
    result_selector = {
        "ClusterId.$": "$.output.ClusterId",
        "ResourceType.$": "$.resourceType",
        "SomethingElse.$": "$.keyDoesntExist",
    }
    task_state = TaskState(
        "Task", resource_uri=dummy_resource_uri, result_selector=result_selector
    )
    state_machine = StateMachine(start_state=task_state)

    # Check the output from compiling
    compiled = compile_state_machine(state_machine)
    assert compiled == {
        "StartAt": task_state.name,
        "States": {
            task_state.name: {
                "Resource": dummy_resource_uri,
                "ResultSelector": result_selector,
                "Type": "Task",
                "End": True,
            },
        },
    }

    # Simulate the state machine

    def mock_fn(_):
        return {
            "resourceType": "elasticmapreduce",
            "resource": "createCluster.sync",
            "output": {
                "SdkHttpMetadata": {
                    "HttpHeaders": {
                        "Content-Length": "1112",
                        "Content-Type": "application/x-amz-JSON-1.1",
                        "Date": "Mon, 25 Nov 2019 19:41:29 GMT",
                        "x-amzn-RequestId": "1234-5678-9012",
                    },
                    "HttpStatusCode": 200,
                },
                "SdkResponseMetadata": {"RequestId": "1234-5678-9012"},
                "ClusterId": "AKIAIOSFODNN7EXAMPLE",
            },
        }

    state_output = state_machine.simulate(
        resource_to_mock_fn={dummy_resource_uri: mock_fn},
    )

    assert state_output == {
        "ResourceType": "elasticmapreduce",
        "ClusterId": "AKIAIOSFODNN7EXAMPLE",
    }


def test_state_has_invalid_result_selector(dummy_resource_uri):
    invalid_result_selector = {"ClusterId.$": "$.dataset*"}
    with pytest.raises(ValueError, match='Unsupported JSONPath operator: "*"'):
        TaskState(
            "My Task",
            resource_uri=dummy_resource_uri,
            result_selector=invalid_result_selector,
        )
