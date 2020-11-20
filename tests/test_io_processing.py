from awsstepfuncs import PassState, StateMachine


def test_input_output_paths(capture_stdout):
    input_path = "$.dataset2"
    output_path = "$.val1"
    pass_state = PassState("Pass 1", input_path=input_path, output_path=output_path)
    state_machine = StateMachine(start_state=pass_state)
    assert state_machine.compile() == {
        "StartAt": "Pass 1",
        "States": {
            "Pass 1": {
                "Type": "Pass",
                "InputPath": "$.dataset2",
                "OutputPath": "$.val1",
                "End": True,
            }
        },
    }
    stdout = capture_stdout(
        lambda: state_machine.simulate(
            {
                "comment": "Example for InputPath.",
                "dataset1": {"val1": 1, "val2": 2, "val3": 3},
                "dataset2": {"val1": "a", "val2": "b", "val3": "c"},
            }
        )
    )
    assert (
        stdout
        == """Starting simulation of state machine
Executing PassState('Pass 1')
State input: {'comment': 'Example for InputPath.', 'dataset1': {'val1': 1, 'val2': 2, 'val3': 3}, 'dataset2': {'val1': 'a', 'val2': 'b', 'val3': 'c'}}
State input after applying input path of $.dataset2: {'val1': 'a', 'val2': 'b', 'val3': 'c'}
Output from applying result path of $: {'val1': 'a', 'val2': 'b', 'val3': 'c'}
State output after applying output path of $.val1: a
State output: a
Terminating simulation of state machine
"""
    )


def test_result_path():
    pass_state = PassState(
        "Passing", result={"Hello": "world!"}, result_path="$.result"
    )
    state_machine = StateMachine(start_state=pass_state)

    assert state_machine.compile() == {
        "StartAt": "Passing",
        "States": {
            "Passing": {
                "Type": "Pass",
                "End": True,
                "ResultPath": "$.result",
                "Result": {"Hello": "world!"},
            }
        },
    }

    state_output = state_machine.simulate({"sum": 42})
    assert state_output == {"sum": 42, "result": {"Hello": "world!"}}
