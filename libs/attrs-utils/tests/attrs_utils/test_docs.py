from dataclasses import dataclass

import pytest

from thds.attrs_utils.docs import get_record_class_fields, record_class_docs


@dataclass
class Record1:  # noqa
    """Record1 doc

    :param a: Record1 a
    :param b: Record1 b
    :param c: Record1 c
    :raises TypeError: Record1 raises
    :return str: Record1 return
    """

    a: int
    b: str
    c: float


@dataclass
class Record2(Record1):  # noqa
    """Record2 doc

    Record2 long desc

    :param b: Record2 b
    :param c: Record2 c
    :param d: Record2 d
    :return int: Record2 return
    """

    b: str
    c: float
    d: bool


@dataclass
class Record3(Record2):  # noqa
    """Record3 doc


    Record3 long desc


    :param a: Record3 a
    :param d: Record3 d
    :param e: Record3 e
    :raises ValueError: Record3 raises 1
    :raises RuntimeError: Record3 raises 2
    """

    a: int
    d: bool
    e: str


# param e missing docstring
@dataclass
class Record4(Record2):
    """Record4 doc

    Record4 long desc

    :param a: Record4 a
    :param d: Record4 d
    :raises ValueError: Record4 raises 1
    :raises RuntimeError: Record4 raises 2
    """

    a: int
    d: bool
    e: str


@pytest.mark.parametrize(
    "c, expected_params, expected_return, expected_return_type, expected_raises, expected_short_desc, expected_long_desc, expected_value_error",
    [
        pytest.param(
            Record3,
            {
                "a": "Record3 a",
                "b": "Record2 b",
                "c": "Record2 c",
                "d": "Record3 d",
                "e": "Record3 e",
            },
            "Record2 return",
            "int",
            [("ValueError", "Record3 raises 1"), ("RuntimeError", "Record3 raises 2")],
            "Record3 doc",
            "Record3 long desc",
            None,
            id="Record3",
        ),
        pytest.param(
            Record4,
            {
                "a": "Record4 a",
                "b": "Record2 b",
                "c": "Record2 c",
                "d": "Record2 d",
            },
            "Record2 return",
            "int",
            [("ValueError", "Record4 raises 1"), ("RuntimeError", "Record4 raises 2")],
            "Record4 doc",
            "Record4 long desc",
            "Missing docstring params for Record4: {'e'}",
            id="Record4",
        ),
    ],
)
def test_record_class_docs(
    c,
    expected_params,
    expected_return,
    expected_return_type,
    expected_raises,
    expected_short_desc,
    expected_long_desc,
    expected_value_error,
):
    if expected_value_error:
        with pytest.raises(ValueError) as e:
            record_class_docs(c, require_complete=True)
        assert str(e.value) == expected_value_error
        return
    else:
        docs = record_class_docs(c, require_complete=True)

        assert {p.arg_name: p.description for p in docs.params} == expected_params
        assert docs.returns is not None
        assert docs.returns.description == expected_return
        assert docs.returns.type_name == expected_return_type
        assert [(r.type_name, r.description) for r in docs.raises] == expected_raises
        assert docs.short_description == expected_short_desc
        assert docs.long_description == expected_long_desc

        expected_short_desc = "Record1 doc\nRecord2 doc\nRecord3 doc"
        expected_long_desc = "Record2 long desc\nRecord3 long desc"
        docs = record_class_docs(Record3, combine_docs="join", join_sep="\n")
        assert docs.short_description == expected_short_desc
        assert docs.long_description == expected_long_desc


@pytest.mark.parametrize(
    "c,expected",
    [
        pytest.param(Record1, {"a", "b", "c"}, id="Record1"),
        pytest.param(Record2, {"a", "b", "c", "d"}, id="Record2"),
        pytest.param(Record3, {"a", "b", "c", "d", "e"}, id="Record3"),
    ],
)
def test_get_record_class_fields(c, expected):
    attrs = get_record_class_fields(c)
    assert attrs == expected
