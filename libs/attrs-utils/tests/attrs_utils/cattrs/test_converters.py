from decimal import Decimal
from typing import AbstractSet, List, Optional, Sequence, Set, Type

import pytest
from cattr.errors import IterableValidationError

from thds.attrs_utils.cattrs import DEFAULT_JSON_CONVERTER, default_converter, setup_converter

from . import conftest

STRICT_CONVERTER = setup_converter(
    default_converter(),
    strict_enums=True,
)


@pytest.mark.parametrize(
    "value, type_, exc_type",
    [
        pytest.param(None, str, TypeError, id="null as str"),
        pytest.param(None, int, TypeError, id="null as int"),
        pytest.param(None, float, TypeError, id="null as float"),
        pytest.param(None, bool, TypeError, id="null as bool"),
        pytest.param(123.4, str, TypeError, id="float as str"),
        pytest.param(123.4, int, TypeError, id="float as int"),
        pytest.param(1.0, bool, TypeError, id="float as bool"),
        pytest.param(345.0, Optional[str], TypeError, id="float as optional str"),
        pytest.param(678.0, Optional[int], TypeError, id="float as optional int"),
        pytest.param(0.0, Optional[bool], TypeError, id="float as optional bool"),
        pytest.param(None, conftest.NT, TypeError, id="null as newtype(str)"),
        pytest.param(123344569.0, conftest.NewNewType, TypeError, id="float as nested newtype(str)"),
        pytest.param(
            235855954.0,
            Optional[conftest.NewNewType],
            TypeError,
            id="float as optional nested newtype(str)",
        ),
        pytest.param(None, conftest.NewNewType, TypeError, id="null as nested newtype(str)"),
        pytest.param([1, "two", 3.0], Set[str], IterableValidationError, id="float in set as set[str]"),
        pytest.param([1, "2", 3.0], Set[int], IterableValidationError, id="float in set as set[int]"),
        pytest.param([True, True, 1], Set[bool], IterableValidationError, id="int in set as set[bool]"),
        pytest.param(
            [True, True, 0.0], Set[bool], IterableValidationError, id="float in set as set[bool]"
        ),
    ],
)
def test_structure_ambiguous_type_conversion_is_forbidden(
    value,
    type_: Type,
    exc_type: Type[Exception],
):
    with pytest.raises(exc_type):
        _ = DEFAULT_JSON_CONVERTER.structure(value, type_)


@pytest.mark.parametrize(
    "value, type_, expected",
    [
        pytest.param(123, float, 123.0, id="int as float"),
        pytest.param("1e3", float, 1000.0, id="str as float"),
        pytest.param(1e3, float, 1000.0, id="float as float"),
        pytest.param(123, str, "123", id="int as str"),
        pytest.param("123", str, "123", id="str as str"),
        pytest.param("123", int, 123, id="str as int"),
        pytest.param(True, int, 1, id="bool as int"),
        pytest.param(1234, int, 1234, id="int as int"),
        pytest.param("123", str, "123", id="str as str"),
        pytest.param(Decimal(1.23), float, 1.23, id="decimal as float"),
        pytest.param(123, Optional[float], 123.0, id="int as optional float"),
        pytest.param("1e3", Optional[float], 1000.0, id="str as optional float"),
        pytest.param(1e3, Optional[float], 1000.0, id="float as optional float"),
        pytest.param(123, Optional[str], "123", id="int as optional str"),
        pytest.param("123", Optional[str], "123", id="str as optional str"),
        pytest.param("123", Optional[int], 123, id="str as optional int"),
        pytest.param(False, Optional[int], 0, id="bool as optional int"),
        pytest.param(1234, Optional[int], 1234, id="int as optional int"),
        pytest.param("123", Optional[str], "123", id="str as optional str"),
        pytest.param(123, conftest.NT, "123", id="int as newtype(str)"),
        pytest.param(123, conftest.NewNewType, "123", id="int as nested newtype(str)"),
        pytest.param(345, Optional[str], "345", id="int as optional str"),
        pytest.param(0, Optional[str], "0", id="int(0) as optional str"),
        pytest.param(Decimal(0.0), Optional[float], 0.0, id="decimal as optional float"),
        pytest.param([1, "two", 3], List[str], ["1", "two", "3"], id="mixed list as list[str]"),
        pytest.param([0, True, "2"], List[int], [0, 1, 2], id="mixed list as list[int]"),
        pytest.param([1.0, 2, "3.4"], List[float], [1.0, 2.0, 3.4], id="mixed list as list[float]"),
        pytest.param(
            [1.0, 2, Decimal("3.4")],
            List[float],
            [1.0, 2.0, 3.4],
            id="mixed list with decimals as list[float]",
        ),
    ],
)
def test_structure_unambiguous_type_conversion_is_allowed(
    value,
    type_: Type,
    expected,
):
    actual = DEFAULT_JSON_CONVERTER.structure(value, type_)
    assert type(actual) is type(expected)
    assert actual == expected, (value, type_, actual, expected)
    if isinstance(expected, Sequence):
        assert all(type(a) is type(b) for a, b in zip(actual, expected))


@pytest.mark.parametrize(
    "value, type_, structured_expected, exc_type",
    [
        # for some reason cattrs raises a bare `Exception` when literal values are invalid
        pytest.param("asdf", conftest.Enum, "asdf", Exception, id="string not in string enum"),
        pytest.param(1, conftest.Enum, "1", Exception, id="int not in string enum"),
        pytest.param(
            ["foo", "bar", "asdf"],
            List[conftest.Enum],
            ["foo", "bar", "asdf"],
            IterableValidationError,
            id="str in set not in string enum",
        ),
    ],
)
def test_structure_enums_strict_vs_nonstrict(
    value, type_: Type, structured_expected, exc_type: Type[Exception]
):
    with pytest.raises(exc_type):
        STRICT_CONVERTER.structure(value, type_)

    structured = DEFAULT_JSON_CONVERTER.structure(value, type_)
    assert structured == structured_expected, (value, type_, structured, structured_expected)


@pytest.mark.parametrize(
    "value, type_, expected",
    [
        pytest.param(None, Optional[conftest.NT], None, id="null as optional newtype"),
        pytest.param("1", conftest.NT, "1", id="str as newtype(str)"),
        pytest.param(123, conftest.NT, "123", id="int as newtype(str)"),
        pytest.param(456, Optional[conftest.NT], "456", id="int as optional newtype(str)"),
        pytest.param("two", conftest.NewNewType, "two", id="str as nested newtype(str)"),
        pytest.param(
            789, Optional[conftest.NewNewType], "789", id="int as optional nested newtype(str)"
        ),
    ],
)
def test_structure_newtype(value, type_: Type, expected):
    result = DEFAULT_JSON_CONVERTER.structure(value, type_)
    assert result == expected, (value, type_, result, expected)


@pytest.mark.parametrize(
    "value, type_, expected",
    [
        pytest.param({6, 3, 5, 1, 2, 4}, Set[int], [1, 2, 3, 4, 5, 6]),
        pytest.param({"g", "a", "d", "e", "b", "f", "c"}, AbstractSet[str], list("abcdefg")),
        pytest.param(
            {
                conftest.Record(conftest.NT("c"), "foo"),
                conftest.Record(conftest.NT("c"), "baz"),
                conftest.Record(conftest.NT("a"), "foo"),
                conftest.Record(conftest.NT("a"), "bar"),
            },
            Set[conftest.Record],
            [
                dict(x="a", y="bar"),
                dict(x="a", y="foo"),
                dict(x="c", y="baz"),
                dict(x="c", y="foo"),
            ],
        ),
    ],
)
def test_unstructure_set(value, type_: Type, expected):
    result = DEFAULT_JSON_CONVERTER.unstructure(value, type_)
    assert result == expected, (value, type_, result, expected)
