from awsstepfuncs import AndChoice, ChoiceRule, NotChoice, PassState, VariableChoice


def test_not_choice():
    next_state = PassState("Passing")
    not_choice = NotChoice(
        variable="$.type",
        string_equals="Private",
        next_state=next_state,
    )
    assert not_choice.evaluate({"type": "Public"})
    assert not not_choice.evaluate({"type": "Private"})
    assert not_choice.evaluate({"sex": "Male"})


def test_and_choice():
    next_state = PassState("Passing")
    and_choice = AndChoice(
        [
            ChoiceRule(variable="$.value", is_present=True),
            ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
            ChoiceRule(variable="$.value", numeric_less_than=30),
        ],
        next_state=next_state,
    )

    assert and_choice.evaluate({"setting": "on", "value": 20})
    assert and_choice.evaluate({"setting": "on", "value": 25})
    assert not and_choice.evaluate({"setting": "on", "value": 30})
    assert not and_choice.evaluate({"setting": "on"})
    assert not and_choice.evaluate({"setting": "on", "value": 50})


def test_variable_choice():
    next_state = PassState("Passing")
    variable_choice = VariableChoice(
        variable="$.type",
        string_equals="Private",
        next_state=next_state,
    )
    assert not variable_choice.evaluate({"type": "Public"})
    assert variable_choice.evaluate({"type": "Private"})

    variable_choice = VariableChoice(
        variable="$.rating",
        numeric_greater_than_path="$.auditThreshold",
        next_state=next_state,
    )
    assert not variable_choice.evaluate({"rating": 53, "auditThreshold": 60})
    assert variable_choice.evaluate({"rating": 53, "auditThreshold": 50})
    assert not variable_choice.evaluate({"rating": 53, "auditThreshold": 53})
