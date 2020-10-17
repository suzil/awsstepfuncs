import re

import pytest

from awsstepfuncs.json_path import apply_json_path


@pytest.fixture(scope="session")
def sample_data():
    return {
        "foo": 123,
        "bar": ["a", "b", "c"],
        "car": {
            "cdr": True,
        },
    }


@pytest.mark.parametrize(
    ("json_path", "match"),
    [("$.foo", 123), ("$.bar", ["a", "b", "c"]), ("$.car.cdr", True)],
)
def test_apply_json_path(json_path, match, sample_data):
    assert apply_json_path(json_path, sample_data) == match


def test_apply_json_path_unsupported_operator(sample_data):
    with pytest.raises(ValueError, match='Unsupported JSONPath operator: "*"'):
        apply_json_path("$foo[*].baz", sample_data)


def test_apply_json_path_must_begin_with_dollar(sample_data):
    with pytest.raises(ValueError, match=re.escape('JSONPath must begin with "$"')):
        apply_json_path("foo[*].baz", sample_data)


def test_apply_json_path_no_match(sample_data):
    assert apply_json_path("$.notfound", sample_data) is None
