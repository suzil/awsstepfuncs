import re

import pytest

from awsstepfuncs.json_path import apply_json_path, validate_json_path


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


@pytest.mark.parametrize(
    "reference_path",
    [
        r"$.store.book",
        r"$.store\.book",
        r"$.\stor\e.boo\k",
        r"$.store.book.title",
        r"$.foo.\.bar",
        # TODO: The following reference path should be valid by example: https://states-language.net/spec.html#ref-paths
        # But there is a "?" which is considered an invalid operator, but perhaps in
        # this scenario it's not considered an operator
        # "$.foo\@bar.baz\[\[.\?pretty",
        r"$.&Ж中.\uD800\uDF46",
        r"$.ledgers.branch[0].pending.count",
        r"$.ledgers.branch[0]",
        r"$.ledgers[0][22][315].foo",
        r"$['store']['book']",
        r"$['store'][0]['book']",
    ],
)
def test_valid_reference_path(reference_path):
    # Should raise no ValueError
    validate_json_path(reference_path)
