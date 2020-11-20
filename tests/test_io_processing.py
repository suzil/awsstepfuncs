from awsstepfuncs import PassState, StateMachine


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
