import pytest

from awsstepfuncs import AWSStepFuncsValueError, ChoiceRule


def test_string_equals_reference_path():
    career_rule = ChoiceRule("$.career", string_equals_path="$.expectedCareer")
    assert career_rule.evaluate({"career": "Pirate", "expectedCareer": "Pirate"})
    assert not career_rule.evaluate({"career": "Pirate", "expectedCareer": "Doctor"})


def test_multiple_data_test_expressions():
    with pytest.raises(
        AWSStepFuncsValueError, match="Exactly one data-test expression must be defined"
    ):
        ChoiceRule("$.career", string_equals="Pirate", is_present=True)


def test_bad_string_equals_path():
    salary_rule = ChoiceRule("$.salary", string_equals_path="$.expectedSalary")
    with pytest.raises(
        AWSStepFuncsValueError,
        match="string_equals_path must evaluate to a string value",
    ):
        salary_rule.evaluate({"salary": "100_000", "expectedSalary": 100_000})


def test_bad_numeric_greater_than_path():
    rating_rule = ChoiceRule("$.rating", numeric_greater_than_path="$.auditThreshold")
    with pytest.raises(
        AWSStepFuncsValueError,
        match="numeric_greater_than_path must evaluate to a numeric value",
    ):
        rating_rule.evaluate({"rating": 53, "auditThreshold": "50"})


def test_string_equals():
    rule = ChoiceRule("$.letter", string_equals="B")
    assert not rule.evaluate({"letter": "A"})
    assert rule.evaluate({"letter": "B"})


def test_string_equals_path():
    rule = ChoiceRule("$.letter", string_equals_path="$.compareLetter")
    assert rule.evaluate({"letter": "A", "compareLetter": "A"})
    assert not rule.evaluate({"letter": "B", "compareLetter": "A"})


def test_string_greater_than():
    rule = ChoiceRule("$.letter", string_greater_than="B")
    assert not rule.evaluate({"letter": "A"})
    assert rule.evaluate({"letter": "C"})


def test_string_greater_than_path():
    rule = ChoiceRule("$.letter", string_greater_than_path="$.compareLetter")
    assert not rule.evaluate({"letter": "A", "compareLetter": "B"})
    assert rule.evaluate({"letter": "C", "compareLetter": "B"})


def test_string_less_than():
    rule = ChoiceRule("$.letter", string_less_than="B")
    assert rule.evaluate({"letter": "A"})
    assert not rule.evaluate({"letter": "C"})


def test_string_less_than_path():
    rule = ChoiceRule("$.letter", string_less_than_path="$.compareLetter")
    assert rule.evaluate({"letter": "A", "compareLetter": "B"})
    assert not rule.evaluate({"letter": "C", "compareLetter": "B"})


def test_string_greater_than_equals():
    rule = ChoiceRule("$.letter", string_greater_than_equals="B")
    assert not rule.evaluate({"letter": "A"})
    assert rule.evaluate({"letter": "B"})
    assert rule.evaluate({"letter": "C"})


def test_string_greater_than_equals_path():
    rule = ChoiceRule("$.letter", string_greater_than_equals_path="$.compareLetter")
    assert not rule.evaluate({"letter": "A", "compareLetter": "B"})
    assert rule.evaluate({"letter": "B", "compareLetter": "B"})
    assert rule.evaluate({"letter": "C", "compareLetter": "B"})


def test_string_less_than_equals():
    rule = ChoiceRule("$.letter", string_less_than_equals="B")
    assert rule.evaluate({"letter": "A"})
    assert rule.evaluate({"letter": "B"})
    assert not rule.evaluate({"letter": "C"})


def test_string_less_than_equals_path():
    rule = ChoiceRule("$.letter", string_less_than_equals_path="$.compareLetter")
    assert rule.evaluate({"letter": "A", "compareLetter": "B"})
    assert rule.evaluate({"letter": "B", "compareLetter": "B"})
    assert not rule.evaluate({"letter": "C", "compareLetter": "B"})
