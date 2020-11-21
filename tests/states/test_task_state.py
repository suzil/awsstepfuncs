import pytest

from awsstepfuncs import AWSStepFuncsValueError, PassState, StateMachine, TaskState


@pytest.fixture(scope="session")
def dummy_resource():
    return "arn:aws:lambda:ap-southeast-2:710187714096:function:DivideNumbers"


def test_task_state(dummy_resource, capture_stdout):
    pass_state = PassState("Pass", comment="The starting state")
    task_state = TaskState("Task", resource=dummy_resource)

    # Define the state machine
    pass_state >> task_state
    state_machine = StateMachine(start_state=pass_state)

    # Check the output from compiling
    assert state_machine.compile() == {
        "StartAt": pass_state.name,
        "States": {
            pass_state.name: {
                "Comment": pass_state.comment,
                "Type": "Pass",
                "Next": task_state.name,
            },
            task_state.name: {
                "Type": "Task",
                "Resource": dummy_resource,
                "End": True,
            },
        },
    }

    # Simulate the state machine

    def mock_fn(event, context):
        event["foo"] *= 2
        return event

    stdout = capture_stdout(
        lambda: state_machine.simulate(
            {"foo": 5, "bar": 1},
            resource_to_mock_fn={dummy_resource: mock_fn},
        )
    )
    assert (
        stdout
        == """Starting simulation of state machine
Executing PassState('Pass')
State input: {'foo': 5, 'bar': 1}
State input after applying input path of $: {'foo': 5, 'bar': 1}
Output from applying result path of $: {'foo': 5, 'bar': 1}
State output after applying output path of $: {'foo': 5, 'bar': 1}
State output: {'foo': 5, 'bar': 1}
Executing TaskState('Task')
State input: {'foo': 5, 'bar': 1}
State input after applying input path of $: {'foo': 5, 'bar': 1}
Output from applying result path of $: {'foo': 10, 'bar': 1}
State output after applying output path of $: {'foo': 10, 'bar': 1}
State output: {'foo': 10, 'bar': 1}
Terminating simulation of state machine
"""
    )


def test_result_selector(dummy_resource):
    result_selector = {
        "ClusterId.$": "$.output.ClusterId",
        "ResourceType.$": "$.resourceType",
        "SomethingElse.$": "$.keyDoesntExist",
    }
    task_state = TaskState(
        "Task", resource=dummy_resource, result_selector=result_selector
    )
    state_machine = StateMachine(start_state=task_state)

    # Check the output from compiling
    assert state_machine.compile() == {
        "StartAt": task_state.name,
        "States": {
            task_state.name: {
                "Resource": dummy_resource,
                "ResultSelector": result_selector,
                "Type": "Task",
                "End": True,
            },
        },
    }

    # Simulate the state machine

    def mock_fn(event, context):
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
        resource_to_mock_fn={dummy_resource: mock_fn},
    )

    assert state_output == {
        "ResourceType": "elasticmapreduce",
        "ClusterId": "AKIAIOSFODNN7EXAMPLE",
    }


def test_result_path_only_state_output(dummy_resource):
    task_state = TaskState("Task", resource=dummy_resource, result_path="$")
    state_machine = StateMachine(start_state=task_state)

    # Check the output from compiling
    assert state_machine.compile() == {
        "StartAt": task_state.name,
        "States": {
            task_state.name: {
                "Resource": dummy_resource,
                "Type": "Task",
                "End": True,
            },
        },
    }

    # Simulate the state machine
    state_input = {
        "comment": "This is a test of the input and output of a Task state.",
        "details": "Default example",
        "who": "AWS Step Functions",
    }

    output_text = "Hello, AWS Step Functions!"

    def mock_fn(event, context):
        return output_text

    state_output = state_machine.simulate(
        state_input,
        resource_to_mock_fn={dummy_resource: mock_fn},
    )

    # Keeps the only the state output
    assert state_output == output_text


def test_result_path_only_state_input(dummy_resource):
    task_state = TaskState("Task", resource=dummy_resource, result_path=None)
    state_machine = StateMachine(start_state=task_state)

    # Check the output from compiling
    assert state_machine.compile() == {
        "StartAt": task_state.name,
        "States": {
            task_state.name: {
                "Resource": dummy_resource,
                "ResultPath": None,
                "Type": "Task",
                "End": True,
            },
        },
    }

    # Simulate the state machine
    state_input = {
        "comment": "This is a test of the input and output of a Task state.",
        "details": "Default example",
        "who": "AWS Step Functions",
    }

    def mock_fn(event, context):
        return "Hello, AWS Step Functions!"

    state_output = state_machine.simulate(
        state_input,
        resource_to_mock_fn={dummy_resource: mock_fn},
    )

    # Keeps the only the state output
    assert state_output == state_input


def test_result_path_keep_both(dummy_resource):
    result_key = "taskresult"
    task_state = TaskState(
        "Task", resource=dummy_resource, result_path=f"$.{result_key}"
    )
    state_machine = StateMachine(start_state=task_state)

    # Check the output from compiling
    assert state_machine.compile() == {
        "StartAt": task_state.name,
        "States": {
            task_state.name: {
                "Resource": dummy_resource,
                "ResultPath": f"$.{result_key}",
                "Type": "Task",
                "End": True,
            },
        },
    }

    # Simulate the state machine
    state_input = {
        "comment": "This is a test of the input and output of a Task state.",
        "details": "Default example",
        "who": "AWS Step Functions",
    }

    output_text = "Hello, AWS Step Functions!"

    def mock_fn(event, context):
        return output_text

    state_output = state_machine.simulate(
        state_input,
        resource_to_mock_fn={dummy_resource: mock_fn},
    )

    state_input[result_key] = output_text

    # Keeps the only the state output
    assert state_output == state_input


def test_state_has_invalid_result_selector(dummy_resource):
    invalid_result_selector = {"ClusterId.$": "$.dataset*"}
    with pytest.raises(
        AWSStepFuncsValueError, match='Unsupported Reference Path operator: "*"'
    ):
        TaskState(
            "My Task",
            resource=dummy_resource,
            result_selector=invalid_result_selector,
        )
