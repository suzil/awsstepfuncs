from awsstepfuncs import PassState, TaskState


def test_compile_parameters():
    y_state = PassState("Y")
    x_state = TaskState(
        "X",
        resource="arn:aws:states:us-east-1:123456789012:task:X",
        parameters={"first": 88, "second": 99},
    )
    _ = x_state >> y_state
    assert x_state.compile() == {
        "Type": "Task",
        "Resource": "arn:aws:states:us-east-1:123456789012:task:X",
        "Next": "Y",
        "Parameters": {"first": 88, "second": 99},
    }
