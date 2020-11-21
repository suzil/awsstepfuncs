from awsstepfuncs import ChoiceRule, FailState, PassState
from awsstepfuncs.choice import DataTestExpression
from awsstepfuncs.errors import FailStateError


def test_data_test_expression_repr():
    assert (
        repr(DataTestExpression("string_equals", "Hello"))
        == "DataTestExpression(string_equals='Hello')"
    )


def test_choice_rule_repr():
    choice_rule = ChoiceRule("$.career", string_equals="Pirate")
    assert repr(choice_rule) == "ChoiceRule('$.career', string_equals='Pirate')"


def test_state_transition_reprs():
    pass_state = PassState("Pass", comment="This is a pass state")
    fail_state = FailState("Fail", error="MyBad", cause="It's a problem")
    pass_state >> fail_state
    assert (
        repr(pass_state)
        == "PassState(name='Pass', comment='This is a pass state', next_state='Fail')"
    )
    assert repr(fail_state) == "FailState(name='Fail')"


def test_fail_state_error_repr():
    fail_state_error = FailStateError(error="IFailed", cause="I failed!")
    assert (
        repr(fail_state_error) == "FailStateError(error='IFailed', cause='I failed!')"
    )
