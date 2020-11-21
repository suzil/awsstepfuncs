from __future__ import annotations

from abc import ABC
from enum import Enum
from typing import Any, List, Union

from awsstepfuncs.abstract_state import AbstractState
from awsstepfuncs.errors import AWSStepFuncsValueError
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
    """A data-test expression."""

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
    """

    def __init__(self, variable: str, **data_test_expression: Any):
        """Initialize a Choice Rule.

        Args:
            variable: The Reference Path to a variable in the state input.
            data_test_expression: The data-test expression to use.

        Raises:
            AWSStepFuncsValueError: Raised when there is not exactly one data-test
                expression defined.
        """
        self.variable = ReferencePath(variable)

        if len(data_test_expression) != 1:
            raise AWSStepFuncsValueError(
                "Exactly one data-test expression must be defined"
            )

        self.data_test_expression = DataTestExpression(
            *list(data_test_expression.items())[0]
        )

    def __repr__(self) -> str:
        """Return a string representation of the Choice Rule.

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
            raise AWSStepFuncsValueError(
                "string_equals_path must evaluate to a string value"
            )
        return variable_value == string_equals

    def _string_greater_than(self, variable_value: str) -> bool:
        return variable_value > self.data_test_expression.expression  # type: ignore

    def _string_greater_than_path(self, data: Any, variable_value: str) -> bool:
        string_greater_than = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_greater_than, str)):  # pragma: no cover
            raise AWSStepFuncsValueError(
                "string_greater_than_path must evaluate to a string value"
            )
        return variable_value > string_greater_than

    def _string_less_than(self, variable_value: str) -> bool:
        return variable_value < self.data_test_expression.expression  # type: ignore

    def _string_less_than_path(self, data: Any, variable_value: str) -> bool:
        string_less_than = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_less_than, str)):  # pragma: no cover
            raise AWSStepFuncsValueError(
                "string_less_than_path must evaluate to a string value"
            )
        return variable_value < string_less_than

    def _string_greater_than_equals(self, variable_value: str) -> bool:
        return variable_value >= self.data_test_expression.expression  # type: ignore

    def _string_greater_than_equals_path(self, data: Any, variable_value: str) -> bool:
        string_greater_than_equals = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_greater_than_equals, str)):  # pragma: no cover
            raise AWSStepFuncsValueError(
                "string_greater_than_equals_path must evaluate to a string value"
            )
        return variable_value >= string_greater_than_equals

    def _string_less_than_equals(self, variable_value: str) -> bool:
        return variable_value <= self.data_test_expression.expression  # type: ignore

    def _string_less_than_equals_path(self, data: Any, variable_value: str) -> bool:
        string_less_than_equals = self.data_test_expression.expression.apply(data)  # type: ignore
        if not (isinstance(string_less_than_equals, str)):  # pragma: no cover
            raise AWSStepFuncsValueError(
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
            raise AWSStepFuncsValueError(
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

    The Not Choice can be evaluated based on input data to true or false based
    on whether the Choice Rule is false.
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

    The And Choice can be evaluated based on input data to true or false based
    on whether all Choice Rules are true.
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

    The Variable Choice can be evaluated based on input data to true or false
    based on whether the Choice Rule is true.

    Be careful if you use a Reference Path that it evaluates to the correct
    type.
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
