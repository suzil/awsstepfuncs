from __future__ import annotations

from abc import ABC
from typing import Any, List, Optional

from awsstepfuncs.abstract_state import AbstractState
from awsstepfuncs.reference_path import ReferencePath


class Condition:
    """Conditions are used in Choices.

    A Condition evalulates to `True` or `False`.

    >>> career_condition = Condition("$.career", string_equals="Pirate")
    >>> career_condition.evaluate({"career": "Pirate", "salary": "10 guineas"})
    True
    >>> career_condition.evaluate({"career": "Sailor", "salary": "5 guineas"})
    False

    A Reference Path can be given too.

    >>> career_condition = Condition("$.career", string_equals_path="$.expectedCareer")
    >>> career_condition.evaluate({"career": "Pirate", "expectedCareer": "Pirate"})
    True
    >>> career_condition.evaluate({"career": "Pirate", "expectedCareer": "Doctor"})
    False

    There can only be one "clause" per condition.

    >>> Condition("$.career", string_equals="Pirate", is_present=True)
    Traceback (most recent call last):
        ...
    ValueError: Exactly one "clause" must be defined

    Be careful that if you specify a Reference Path that it evaluates to a value
    with the expected type.

    >>> salary_condition = Condition("$.salary", string_equals_path="$.expectedSalary")
    >>> salary_condition.evaluate({"salary": "100_000", "expectedSalary": 100_000})
    Traceback (most recent call last):
        ...
    ValueError: string_equals_path must evaluate to a string value
    """

    def __init__(
        self,
        variable: str,
        *,
        string_equals: Optional[str] = None,
        string_equals_path: Optional[str] = None,
        is_present: Optional[bool] = None,
        numeric_greater_than_equals: Optional[int] = None,
        numeric_greater_than_path: Optional[str] = None,
        numeric_less_than: Optional[int] = None,
    ):
        """Initialize a Condition.

        Args:
            variable: The Reference Path to a variable in the state input.
            string_equals: If set, whether or not the variable equals the
                string.
            string_equals_path: If set, whether or not the variable equals the
                string at the Reference Path.
            is_present: If set, whether the variable is present.
            numeric_greater_than_equals: If set, whether the variable is greater
                than or equal to the numeric value.
            numeric_greater_than_path: If set, whether the variable is greater
                than the value at the Reference Path.
            numeric_less_than: If set, whether the variable is less than the
                value.

        Raises:
            ValueError: Raised when there is not exactly one "clause" defined.
        """
        self.variable = ReferencePath(variable)

        if (
            sum(
                variable is not None
                for variable in [
                    string_equals,
                    string_equals_path,
                    is_present,
                    numeric_greater_than_equals,
                    numeric_greater_than_path,
                    numeric_less_than,
                ]
            )
            != 1
        ):
            raise ValueError('Exactly one "clause" must be defined')

        self.string_equals = string_equals
        self.string_equals_path = (
            ReferencePath(string_equals_path) if string_equals_path else None
        )
        self.is_present = is_present
        self.numeric_greater_than_equals = numeric_greater_than_equals
        self.numeric_greater_than_path = (
            ReferencePath(numeric_greater_than_path)
            if numeric_greater_than_path
            else None
        )
        self.numeric_less_than = numeric_less_than

    def __repr__(self) -> str:
        """Return a string representation of the Condition.

        >>> Condition("$.career", string_equals="Pirate")
        Condition('$.career', string_equals='Pirate')

        >>> Condition("$.career", string_equals_path="$.expectedCareer")
        Condition('$.career', string_equals_path='$.expectedCareer')

        >>> Condition("$.career", is_present=True)
        Condition('$.career', is_present=True)

        >>> Condition("$.rating", numeric_greater_than_equals=42)
        Condition('$.rating', numeric_greater_than_equals=42)

        >>> Condition("$.rating", numeric_greater_than_path="$.threshold")
        Condition('$.rating', numeric_greater_than_path='$.threshold')

        >>> Condition("$.rating", numeric_less_than=30)
        Condition('$.rating', numeric_less_than=30)

        Returns:
            A string representing the Condition.
        """
        clauses = self.__dict__.copy()
        variable = clauses.pop("variable")
        clauses_formatted = ", ".join(
            f"{name}={value!r}" for name, value in clauses.items() if value is not None
        )
        return f"{self.__class__.__name__}({variable!r}, {clauses_formatted})"

    def evaluate(self, data: Any) -> bool:
        """Evaulate the condition on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            True or false based on the data and the Condition.
        """
        variable_value = self.variable.apply(data)

        if self.is_present:
            return self._is_present(variable_value)

        if variable_value is None:
            return False

        if self.string_equals:
            return self._string_equals(variable_value)

        if self.string_equals_path:
            return self._string_equals_path(data, variable_value)

        if self.numeric_greater_than_equals:
            return self._numeric_greater_than_equals(variable_value)

        if self.numeric_greater_than_path:
            return self._numeric_greater_than_path(data, variable_value)

        if self.numeric_less_than:
            return self._numeric_less_than(variable_value)

        assert False, "Not yet supported operation"  # noqa: PT015 pragma: no cover

    def _is_present(self, variable_value: Any) -> bool:
        return variable_value is not None

    def _string_equals(self, variable_value: Any) -> bool:
        return variable_value == self.string_equals

    def _string_equals_path(self, data: Any, variable_value: Any) -> bool:
        string_equals = self.string_equals_path.apply(data)  # type: ignore
        if not (isinstance(string_equals, str)):
            raise ValueError("string_equals_path must evaluate to a string value")
        return variable_value == string_equals

    def _numeric_greater_than_equals(self, variable_value: Any) -> bool:
        return variable_value >= self.numeric_greater_than_equals

    def _numeric_greater_than_path(self, data: Any, variable_value: Any) -> bool:
        numeric_greater_than = self.numeric_greater_than_path.apply(data)  # type: ignore
        if not (
            isinstance(numeric_greater_than, int)
            or isinstance(numeric_greater_than, float)
        ):
            raise ValueError(
                "numeric_greater_than_path must evaluate to a numeric value"
            )
        return variable_value > numeric_greater_than

    def _numeric_less_than(self, variable_value: Any) -> bool:
        return variable_value < self.numeric_less_than


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
    on whether the condition is false.

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
        string_equals: Optional[str] = None,
        string_equals_path: Optional[str] = None,
        is_present: Optional[bool] = None,
        numeric_greater_than_equals: Optional[int] = None,
        numeric_greater_than_path: Optional[str] = None,
        numeric_less_than: Optional[int] = None,
    ):
        """Initialize a NotChoice.

        Args:
            variable: The Reference Path to a variable in the state input.
            next_state: The state to transition to if evaluated to true.
            string_equals: If set, whether or not the variable equals the
                string.
            string_equals_path: If set, whether or not the variable equals the
                string at the Reference Path.
            is_present: If set, whether the variable is present.
            numeric_greater_than_equals: If set, whether the variable is greater
                than or equal to the numeric value.
            numeric_greater_than_path: If set, whether the variable is greater
                than the value at the Reference Path.
            numeric_less_than: If set, whether the variable is less than the
                value.
        """
        super().__init__(next_state)
        self.condition = Condition(
            variable,
            string_equals=string_equals,
            string_equals_path=string_equals_path,
            is_present=is_present,
            numeric_greater_than_equals=numeric_greater_than_equals,
            numeric_greater_than_path=numeric_greater_than_path,
            numeric_less_than=numeric_less_than,
        )

    def evaluate(self, data: Any) -> bool:
        """Evaulate the Not Choice on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            Whether the choice evaluates to true based on the input data.
        """
        return not self.condition.evaluate(data)


class AndChoice(AbstractChoice):
    """And Choice for the Choice State.

    >>> from awsstepfuncs import *
    >>> next_state = PassState("Passing")
    >>> and_choice = AndChoice(
    ...     [
    ...         Condition(variable="$.value", is_present=True),
    ...         Condition(variable="$.value", numeric_greater_than_equals=20),
    ...         Condition(variable="$.value", numeric_less_than=30),
    ...     ],
    ...     next_state=next_state,
    ... )

    The And Choice can be evaluated based on input data to true or false based
    on whether all conditions are true.

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
        conditions: List[Condition],
        *,
        next_state: AbstractState,
    ):
        """Initialize an AndChoice.

        Args:
            conditions: A list of conditions which must ALL evaluate to true.
            next_state: The state to transition to if true.
        """
        super().__init__(next_state)
        self.conditions = conditions

    def evaluate(self, data: Any) -> bool:
        """Evaulate the And Choice on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            Whether the choice evaluates to true based on the input data.
        """
        return all(condition.evaluate(data) for condition in self.conditions)


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
    based on whether the condition is true.

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
        string_equals: Optional[str] = None,
        string_equals_path: Optional[str] = None,
        is_present: Optional[bool] = None,
        numeric_greater_than_equals: Optional[int] = None,
        numeric_greater_than_path: Optional[str] = None,
        numeric_less_than: Optional[int] = None,
    ):
        """Initialize a VariableChoice.

        Args:
            variable: The Reference Path to a variable in the state input.
            next_state: The state to transition to if evaluated to true.
            string_equals: If set, whether or not the variable equals the
                string.
            string_equals_path: If set, whether or not the variable equals the
                string at the Reference Path.
            is_present: If set, whether the variable is present.
            numeric_greater_than_equals: If set, whether the variable is greater
                than or equal to the numeric value.
            numeric_greater_than_path: If set, whether the variable is greater
                than the value at the Reference Path.
            numeric_less_than: If set, whether the variable is less than the
                value.
        """
        super().__init__(next_state)
        self.condition = Condition(
            variable,
            string_equals=string_equals,
            string_equals_path=string_equals_path,
            is_present=is_present,
            numeric_greater_than_equals=numeric_greater_than_equals,
            numeric_greater_than_path=numeric_greater_than_path,
            numeric_less_than=numeric_less_than,
        )

    def evaluate(self, data: Any) -> bool:
        """Evaulate the Variable Choice on some given data.

        Args:
            data: Input data to evaluate.

        Returns:
            Whether the choice evaluates to true based on the input data.
        """
        return self.condition.evaluate(data)
