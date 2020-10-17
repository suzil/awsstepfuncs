from typing import Any

from jsonpath_rw import parse as parse_jsonpath


def apply_json_path(json_path: str, data: dict) -> Any:
    """Parse then apply a JSONPath on some data.

    Args:
        json_path: The JSONPath to parse and apply.
        data: The data to use the JSONPath expression on.

    Raises:
        ValueError: Raised when the JSONPath is invalid (for the subset that
            Amazon States Language uses).

    Returns:
        The queried data.
    """
    try:
        validate_json_path(json_path)
    except ValueError:
        raise

    parsed_json_path = parse_jsonpath(json_path)
    if matches := [match.value for match in parsed_json_path.find(data)]:
        return matches[0]


def validate_json_path(json_path: str) -> None:
    """Validate a JSONPath for Amazon States Language.

    Args:
        json_path: The JSONPath to validate.

    Raises:
        ValueError: Raised when JSONPath is an empty string or does not
            begin with a "$".
        ValueError: Raised when the JSONPath has an unsupported operator (an
            operator that Amazon States Language does not support).
    """
    if not json_path or json_path[0] != "$":
        raise ValueError('JSONPath must begin with "$"')

    unsupported_operators = {"@", "..", ",", ":", "?", "*"}
    for operator in unsupported_operators:
        if operator in json_path:
            raise ValueError(f'Unsupported JSONPath operator: "{operator}"')
