from __future__ import annotations

from abc import ABC
from enum import Enum
from typing import Any, List, Union

from awsstepfuncs.abstract_state import AbstractState
from awsstepfuncs.reference_path import ReferencePath


class DataTestExpressionType(Enum):
    """All the different types of data-test expressions.

    Check section "Data-test expression" for a full list:
    https://states-language.net/#choice-state
    """

    STRING_EQUALS = "string_equals"
    STRING_EQUALS_PATH = "string_equals_path"
    STRING_LESS_THAN = "string_less_than"
    STRING_LESS_THAN_PATH = "string_less_than_path"
    STRING_GREATER_THAN = "string_greater_than"
    STRING_GREATER_THAN_PATH = "string_greater_than_path"
    STRING_LESS_THAN_EQUALS = "string_less_than_equals"
    STRING_LESS_THAN_EQUALS_PATH = "string_less_than_equals_path"
    STRING_GREATER_THAN_EQUALS = "string_greater_than_equals"
    STRING_GREATER_THAN_EQUALS_PATH = "string_greater_than_equals_path"
    STRING_MATCHES = "string_matches"
    NUMERIC_EQUALS = "numeric_equals"
    NUMERIC_EQUALS_PATH = "numeric_equals_path"
    NUMERIC_LESS_THAN = "numeric_less_than"
    NUMERIC_LESS_THAN_PATH = "numeric_less_than_path"
    NUMERIC_GREATER_THAN = "numeric_greater_than"
    NUMERIC_GREATER_THAN_PATH = "numeric_greater_than_path"
    NUMERIC_LESS_THAN_EQUALS = "numeric_less_than_equals"
    NUMERIC_LESS_THAN_EQUALS_PATH = "numeric_less_than_equals_path"
    NUMERIC_GREATER_THAN_EQUALS = "numeric_greater_than_equals"
    NUMERIC_GREATER_THAN_EQUALS_PATH = "numeric_greater_than_equals_path"
    BOOLEAN_EQUALS = "boolean_equals"
    BOOLEAN_EQUALS_PATH = "boolean_equals_path"
    TIMESTAMP_EQUALS = "timestamp_equals"
    TIMESTAMP_EQUALS_PATH = "timestamp_equals_path"
    TIMESTAMP_LESS_THAN = "timestamp_less_than"
    TIMESTAMP_LESS_THAN_PATH = "timestamp_less_than_path"
    TIMESTAMP_GREATER_THAN = "timestamp_greater_than"
    TIMESTAMP_GREATER_THAN_PATH = "timestamp_greater_than_path"
    TIMESTAMP_LESS_THAN_EQUALS = "timestamp_less_than_equals"
    TIMESTAMP_LESS_THAN_EQUALS_PATH = "timestamp_less_than_equals_path"
    TIMESTAMP_GREATER_THAN_EQUALS = "timestamp_greater_than_equals"
    TIMESTAMP_GREATER_THAN_EQUALS_PATH = "timestamp_greater_than_equals_path"
    IS_NULL = "is_null"
    IS_PRESENT = "is_present"
    IS_NUMERIC = "is_numeric"
    IS_STRING = "is_string"
    IS_BOOLEAN = "is_boolean"
    IS_TIMESTAMP = "is_timestamp"


class DataTestExpression:
    """A data-test expression.

    >>> DataTestExpression("string_equals", "Hello")
    DataTestExpression(string_equals='Hello')
    """

    def __init__(self, type: str, expression: Any):  # noqa: A002
        """Initialize a data-test expression.

        Args:
            type: The type of data-test expression, such as string_equals.
            expression: The expression to use when evaluating based on the type.
        """
        # NOTE: The enum is just used for validation
        self.type = DataTestExpressionType(type).value
        self.expression = ReferencePath(expression) if "path" in type else expression

    def __repr__(self) -> str:
        """A string representation of a data-test expression."""
        return f"{self.__class__.__name__}({self.type}={self.expression!r})"


class ChoiceRule:
    """Choice Rules are used in Choices.

    When initializing a Choice Rule, a data test expression must be provided. A
    Choice Rule evalulates to `True` or `False` based on the data-test
    expression on some data.

    >>> career_rule = ChoiceRule("$.career", string_equals="Pirate")
    >>> career_rule.evaluate({"career": "Pirate", "salary": "10 guineas"})
    True
    >>> career_rule.evaluate({"career": "Sailor", "salary": "5 guineas"})
    False

    A Reference Path can be given too.

    >>> career_rule = ChoiceRule("$.career", string_equals_path="$.expectedCareer")
    >>> career_rule.evaluate({"career": "Pirate", "expectedCareer": "Pirate"})
    True
    >>> career_rule.evaluate({"career": "Pirate", "expectedCareer": "Doctor"})
    False

    There can only be one data-test expression per Choice Rule.

    >>> ChoiceRule("$.career", string_equals="Pirate", is_present=True)
    Traceback (most recent call last):
        ...
    ValueError: Exactly one data-test expression must be defined

    Be careful that if you specify a Reference Path that it evaluates to a value
    with the expected type.

    >>> salary_rule = ChoiceRule("$.salary", string_equals_path="$.expectedSalary")
    >>> salary_rule.evaluate({"salary": "100_000", "expectedSalary": 100_000})
    Traceback (most recent call last):
        ...
    ValueError: string_equals_path must evaluate to a string value

    There are many different data-test expressions to choose from:

    **string_equals**

    >>> rule = ChoiceRule("$.letter", string_equals="B")
    >>> rule.evaluate({"letter": "A"})
    False
    >>> rule.evaluate({"letter": "B"})
    True

    **string_equals_path**

    >>> rule = ChoiceRule("$.letter", string_equals_path="$.compareLetter")
    >>> rule.evaluate({"letter": "A", "compareLetter": "A"})
    True
    >>> rule.evaluate({"letter": "B", "compareLetter": "A"})
    False

    **string_greater_than**

    >>> rule = ChoiceRule("$.letter", string_greater_than="B")
    >>> rule.evaluate({"letter": "A"})
    False
    >>> rule.evaluate({"letter": "C"})
    True

    **string_greater_than_path**

    >>> rule = ChoiceRule("$.letter", string_greater_than_path="$.compareLetter")
    >>> rule.evaluate({"letter": "A", "compareLetter": "B"})
    False
    >>> rule.evaluate({"letter": "C", "compareLetter": "B"})
    True

    **string_less_than**

    >>> rule = ChoiceRule("$.letter", string_less_than="B")
    >>> rule.evaluate({"letter": "A"})
    True
    >>> rule.evaluate({"letter": "C"})
    False

    **string_less_than_path**

    >>> rule = ChoiceRule("$.letter", string_less_than_path="$.compareLetter")
    >>> rule.evaluate({"letter": "A", "compareLetter": "B"})
    True
    >>> rule.evaluate({"letter": "C", "compareLetter": "B"})
    False

    **string_greater_than_equals**

    >>> rule = ChoiceRule("$.letter", string_greater_than_equals="B")
    >>> rule.evaluate({"letter": "A"})
    False
    >>> rule.evaluate({"letter": "B"})
    True
    >>> rule.evaluate({"letter": "C"})
    True

    **string_greater_than_equals_path**

    >>> rule = ChoiceRule("$.letter", string_greater_than_equals_path="$.compareLetter")
    >>> rule.evaluate({"letter": "A", "compareLetter": "B"})
    False
    >>> rule.evaluate({"letter": "B", "compareLetter": "B"})
    True
    >>> rule.evaluate({"letter": "C", "compareLetter": "B"})
    True

    **string_less_than_equals**

    >>> rule = ChoiceRule("$.letter", string_less_than_equals="B")
    >>> rule.evaluate({"letter": "A"})
    True
    >>> rule.evaluate({"letter": "B"})
    True
    >>> rule.evaluate({"letter": "C"})
    False

    **string_less_than_equals_path**

    >>> rule = ChoiceRule("$.letter", string_less_than_equals_path="$.compareLetter")
    >>> rule.evaluate({"letter": "A", "compareLetter": "B"})
    True
    >>> rule.evaluate({"letter": "B", "compareLetter": "B"})
    True
    >>> rule.evaluate({"letter": "C", "compareLetter": "B"})
    False
    """

    def __init__(self, variable: str, **data_test_expression: Any):
        """Initialize a Choice Rule.

        Args:
            variable: The Reference Path to a variable in the state input.
            data_test_expression: The data-test expression to use.

        Raises:
            ValueError: Raised when there is not exactly one data-test
                expression defined.
        """
        self.variable = ReferencePath(variable)

        if len(data_test_expression) != 1:
            raise ValueError("Exactly one data-test expression must be defined")

        self.data_test_expression = DataTestExpression(
            *list(data_test_expression.items())[0]
        )

    def __repr__(self) -> str:
        """Return a string representation of the Choice Rule.

        >>> ChoiceRule("$.career", string_equals="Pirate")
        ChoiceRule('$.career', string_equals='Pirate')

        Returns:
            A string representing the Choice Rule.
        """
        return f"{self.__class__.__name__}({self.variable!r}, {self.data_test_expression.type}={self.data_test_expression.expression!r})"

    def evaluate(self, data: Any) -> bool:
        """Evaulate the Choice Rule with a data-test expression on some data.

        Args:
            data: Input data to evaluate.

        Returns:
            True or false based on the data and the Choice Rule.
        """
        variable_value = self.variable.apply(data)

        if variable_value is None:
            return False

        if "path" in self.data_test_expression.type:
            return eval(f"self._{self.data_test_expression.type}(data, variable_value)")
        else:
            return eval(f"self._{self.data_test_expression.type}(variable_value)")

    def _is_present(self, variable_value: Any) -> bool:
        return variable_value is not None

    def _string_equals(self, variable_value: str) -> bool:
        return variable_value == self.data_test_expression.expression

    def _string_equals_path(self, data: Any, variable_value: str) -> bool:
        string_equals = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_equals, str)):
            raise ValueError("string_equals_path must evaluate to a string value")
        return variable_value == string_equals

    def _string_greater_than(self, variable_value: str) -> bool:
        return variable_value > self.data_test_expression.expression  # type: ignore

    def _string_greater_than_path(self, data: Any, variable_value: str) -> bool:
        string_greater_than = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_greater_than, str)):  # pragma: no cover
            raise ValueError("string_greater_than_path must evaluate to a string value")
        return variable_value > string_greater_than

    def _string_less_than(self, variable_value: str) -> bool:
        return variable_value < self.data_test_expression.expression  # type: ignore

    def _string_less_than_path(self, data: Any, variable_value: str) -> bool:
        string_less_than = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_less_than, str)):  # pragma: no cover
            raise ValueError("string_less_than_path must evaluate to a string value")
        return variable_value < string_less_than

    def _string_greater_than_equals(self, variable_value: str) -> bool:
        return variable_value >= self.data_test_expression.expression  # type: ignore

    def _string_greater_than_equals_path(self, data: Any, variable_value: str) -> bool:
        string_greater_than_equals = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_greater_than_equals, str)):  # pragma: no cover
            raise ValueError(
                "string_greater_than_equals_path must evaluate to a string value"
            )
        return variable_value >= string_greater_than_equals

    def _string_less_than_equals(self, variable_value: str) -> bool:
        return variable_value <= self.data_test_expression.expression  # type: ignore

    def _string_less_than_equals_path(self, data: Any, variable_value: str) -> bool:
        string_less_than_equals = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_less_than_equals, str)):  # pragma: no cover
            raise ValueError(
                "string_less_than_equals_path must evaluate to a string value"
            )
        return variable_value <= string_less_than_equals

    def _numeric_greater_than_equals(self, variable_value: Union[float, int]) -> bool:
        return variable_value >= self.data_test_expression.expression  # type: ignore

    def _numeric_greater_than_path(
        self, data: Any, variable_value: Union[float, int]
    ) -> bool:
        numeric_greater_than = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (
            isinstance(numeric_greater_than, int)
            or isinstance(numeric_greater_than, float)
        ):
            raise ValueError(
                "numeric_greater_than_path must evaluate to a numeric value"
            )
        return variable_value > numeric_greater_than

    def _numeric_less_than(self, variable_value: Union[float, int]) -> bool:
        return variable_value < self.data_test_expression.expression  # type: ignore


class AbstractChoice(ABC):
    """Choices for Choice State."""

    def __init__(self, next_state: AbstractState):
        """Perform common initialization steps for all choices.

        Args:
            next_state: The state that the choice should transition to if true.
        """
        self.next_state = next_state

    def evaluate(self, data: Any) -> bool:
        """Evaulate the choice on some given data.

        Args:
            data: Input data to evaluate.

        Raises:
            NotImplementedError: Raised if not implemented in child classes.
        """
        raise NotImplementedError


class NotChoice(AbstractChoice):
    """Not choice for the Choice State.

    >>> from awsstepfuncs import *
    >>> next_state = PassState("Passing")
    >>> not_choice = NotChoice(
    ...     variable="$.type",
    ...     string_equals="Private",
    ...     next_state=next_state,
    ... )

    The Not Choice can be evaluated based on input data to true or false based
    on whether the Choice Rule is false.

    >>> not_choice.evaluate({"type": "Public"})
    True
    >>> not_choice.evaluate({"type": "Private"})
    False
    >>> not_choice.evaluate({"sex": "Male"})
    True
    """

    def __init__(
        self,
        variable: str,
        *,
        next_state: AbstractState,
        **data_test_expression: Any,
    ):
        """Initialize a NotChoice.

        Args:
            variable: The Reference Path to a variable in the state input.
            next_state: The state to transition to if evaluated to true.
            data_test_expression: The data-test expression to use.
        """
        super().__init__(next_state)
        self.choice_rule = ChoiceRule(
            variable,
            **data_test_expression,
        )

    def evaluate(self, data: Any) -> bool:
        """Evaulate the Not Choice on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            Whether the choice evaluates to true based on the input data.
        """
        return not self.choice_rule.evaluate(data)


class AndChoice(AbstractChoice):
    """And Choice for the Choice State.

    >>> from awsstepfuncs import *
    >>> next_state = PassState("Passing")
    >>> and_choice = AndChoice(
    ...     [
    ...         ChoiceRule(variable="$.value", is_present=True),
    ...         ChoiceRule(variable="$.value", numeric_greater_than_equals=20),
    ...         ChoiceRule(variable="$.value", numeric_less_than=30),
    ...     ],
    ...     next_state=next_state,
    ... )

    The And Choice can be evaluated based on input data to true or false based
    on whether all Choice Rules are true.

    >>> and_choice.evaluate({"setting": "on", "value": 20})
    True
    >>> and_choice.evaluate({"setting": "on", "value": 25})
    True
    >>> and_choice.evaluate({"setting": "on", "value": 30})
    False
    >>> and_choice.evaluate({"setting": "on"})
    False
    >>> and_choice.evaluate({"setting": "on", "value": 50})
    False
    """

    def __init__(
        self,
        choice_rules: List[ChoiceRule],
        *,
        next_state: AbstractState,
    ):
        """Initialize an AndChoice.

        Args:
            choice_rules: A list of Choice Rules which must ALL evaluate to true.
            next_state: The state to transition to if true.
        """
        super().__init__(next_state)
        self.choice_rules = choice_rules

    def evaluate(self, data: Any) -> bool:
        """Evaulate the And Choice on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            Whether the choice evaluates to true based on the input data.
        """
        return all(choice_rule.evaluate(data) for choice_rule in self.choice_rules)


class VariableChoice(AbstractChoice):
    """Variable Choice for the Choice State.

    >>> from awsstepfuncs import *
    >>> next_state = PassState("Passing")
    >>> variable_choice = VariableChoice(
    ...     variable="$.type",
    ...     string_equals="Private",
    ...     next_state=next_state,
    ... )

    The Variable Choice can be evaluated based on input data to true or false
    based on whether the Choice Rule is true.

    >>> variable_choice.evaluate({"type": "Public"})
    False
    >>> variable_choice.evaluate({"type": "Private"})
    True

    Here's another example:

    >>> variable_choice = VariableChoice(
    ...     variable="$.rating",
    ...     numeric_greater_than_path="$.auditThreshold",
    ...     next_state=next_state,
    ... )
    >>> variable_choice.evaluate({"rating": 53, "auditThreshold": 60})
    False
    >>> variable_choice.evaluate({"rating": 53, "auditThreshold": 50})
    True
    >>> variable_choice.evaluate({"rating": 53, "auditThreshold": 53})
    False

    Be careful if you use a Reference Path that it evaluates to the correct
    type.

    >>> variable_choice.evaluate({"rating": 53, "auditThreshold": "50"})
    Traceback (most recent call last):
        ...
    ValueError: numeric_greater_than_path must evaluate to a numeric value
    """

    def __init__(
        self,
        variable: str,
        *,
        next_state: AbstractState,
        **data_test_expression: Any,
    ):
        """Initialize a VariableChoice.

        Args:
            variable: The Reference Path to a variable in the state input.
            next_state: The state to transition to if evaluated to true.
            data_test_expression: The data-test expression to use.
        """
        super().__init__(next_state)
        self.choice_rule = ChoiceRule(
            variable,
            **data_test_expression,
        )

    def evaluate(self, data: Any) -> bool:
        """Evaulate the Variable Choice on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            Whether the choice evaluates to true based on the input data.
        """
        return self.choice_rule.evaluate(data)
