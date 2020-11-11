from typing import Any

from jsonpath_rw import parse as parse_jsonpath


class ReferencePath:
    """Reference Path validation and application.

    Reference Path is a specialized JSONPath. Unlike a JSONPath, the Reference
    Path must be unambiguous and evaluate to only a single node.

    More on Reference Paths: https://states-language.net/spec.html#ref-paths
    More on JSONPath: https://github.com/json-path/JsonPath
    """

    def __init__(self, reference_path: str, /):
        """Initialize a Reference Path.

        >>> reference_path = ReferencePath("$.detail.sum")
        >>> reference_path.apply({"show": True, "detail": {"mean": 10.4, "sum": 2000}})
        2000

        Args:
            reference_path: The Reference Path string to use (a JSONPath).

        Raises:
            ValueError: Raised when the Reference Path is malformed.
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
            The string representation of the Reference Path.
        """
        return repr(self.reference_path)

    def __str__(self) -> str:
        """Return the Reference Path string (a JSONPAth).

        >>> reference_path = ReferencePath("$.detail.sum")
        >>> print(reference_path)
        $.detail.sum

        Returns:
            The human-readable string representation of the Reference Path.
        """
        return self.reference_path

    def __bool__(self) -> bool:
        """Whether the Reference Path has something besides $ (default)."""
        return self.reference_path != "$"

    def _validate(self) -> None:
        """Validate a Reference Path for Amazon States Language.

        Raises:
            ValueError: Raised when Reference Path is an empty string or does not
                begin with a "$".
            ValueError: Raised when the Reference Path has an unsupported operator (an
                operator that Amazon States Language does not support).
        """
        if not self.reference_path or str(self.reference_path)[0] != "$":
            raise ValueError('Reference Path must begin with "$"')

        unsupported_operators = {"@", "..", ",", ":", "?", "*"}
        for operator in unsupported_operators:
            if operator in str(self.reference_path):
                raise ValueError(f'Unsupported Reference Path operator: "{operator}"')

    def apply(self, data: dict) -> Any:
        """Parse then apply a Reference Path on some data.

        Args:
            data: The data to use the Reference Path expression on.

        Returns:
            The queried data.
        """
        parsed_reference_path = parse_jsonpath(self.reference_path)
        if matches := [match.value for match in parsed_reference_path.find(data)]:
            assert len(matches) == 1, "There should only be one match possible"
            return matches[0]
