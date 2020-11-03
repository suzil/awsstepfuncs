from typing import Any

from jsonpath_rw import parse as parse_jsonpath


class ReferencePath:
    """ReferencePath validation and application.

    See: https://github.com/json-path/JsonPath
    """

    def __init__(self, reference_path: str, /):
        """Initialize a ReferencePath.

        >>> reference_path = ReferencePath("$.detail.sum")
        >>> reference_path.apply({"show": True, "detail": {"mean": 10.4, "sum": 2000}})
        2000

        Args:
            reference_path: The ReferencePath string to use.

        Raises:
            ValueError: Raised when the ReferencePath is malformed.
        """
        self.reference_path = reference_path or "$"
        try:
            self._validate()
        except ValueError:
            raise

    def __repr__(self) -> str:
        """Return the string representation of the class.

        >>> ReferencePath("$.detail.sum")
        '$.detail.sum'

        Returns:
            The string representation of the ReferencePath.
        """
        return repr(self.reference_path)

    def __str__(self) -> str:
        """Return the ReferencePath string.

        >>> reference_path = ReferencePath("$.detail.sum")
        >>> print(reference_path)
        $.detail.sum

        Returns:
            The human-readable string representation of the ReferencePath.
        """
        return self.reference_path

    def __bool__(self) -> bool:
        """Whether the ReferencePath has something besides $ (default)."""
        return self.reference_path != "$"

    def _validate(self) -> None:
        """Validate a ReferencePath for Amazon States Language.

        Raises:
            ValueError: Raised when ReferencePath is an empty string or does not
                begin with a "$".
            ValueError: Raised when the ReferencePath has an unsupported operator (an
                operator that Amazon States Language does not support).
        """
        if not self.reference_path or str(self.reference_path)[0] != "$":
            raise ValueError('ReferencePath must begin with "$"')

        unsupported_operators = {"@", "..", ",", ":", "?", "*"}
        for operator in unsupported_operators:
            if operator in str(self.reference_path):
                raise ValueError(f'Unsupported ReferencePath operator: "{operator}"')

    def apply(self, data: dict) -> Any:
        """Parse then apply a ReferencePath on some data.

        Args:
            data: The data to use the ReferencePath expression on.

        Returns:
            The queried data.
        """
        parsed_reference_path = parse_jsonpath(self.reference_path)
        if matches := [match.value for match in parsed_reference_path.find(data)]:
            assert len(matches) == 1, "There should only be one match possible"
            return matches[0]
