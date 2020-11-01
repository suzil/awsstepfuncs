import re

import pytest

from awsstepfuncs.json_path import ReferencePath


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
    assert ReferencePath(json_path).apply(sample_data) == match


def test_json_path_unsupported_operator():
    with pytest.raises(ValueError, match='Unsupported ReferencePath operator: "*"'):
        ReferencePath("$foo[*].baz")


def test_json_path_must_begin_with_dollar():
    with pytest.raises(
        ValueError, match=re.escape('ReferencePath must begin with "$"')
    ):
        ReferencePath("foo[*].baz")


def test_apply_json_path_no_match(sample_data):
    assert ReferencePath("$.notfound").apply(sample_data) is None


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
    # Should not raise any ValueError
    ReferencePath(reference_path)
