from awsstepfuncs import FailState, PassState


def test_repr():
    pass_state = PassState("Pass", comment="This is a pass state")
    fail_state = FailState("Fail", error="MyBad", cause="It's a problem")
    _ = pass_state >> fail_state
    assert (
        repr(pass_state)
        == "PassState(name='Pass', comment='This is a pass state', next_state='Fail')"
    )
    assert repr(fail_state) == "FailState(name='Fail')"
