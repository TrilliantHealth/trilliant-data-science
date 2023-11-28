"""Test structured recursion by implementing a JSON serializer and testing that the stdlib `json.loads`
is an inverse."""
import json
import math
from typing import Any, Mapping, Sequence

import pytest
from returns.curry import partial

from thds.attrs_utils.recursion import F, Predicate, StructuredRecursion, value_error


def mapping_to_json(to_json: "F[Any, [str], str]", value: Mapping, sep: str = " "):
    return "{{{}}}".format(
        ("," + sep).join(f"{to_json(k, sep)}:{sep}{to_json(v, sep)}" for k, v in value.items())
    )


def sequence_to_json(to_json: "F[Any, [str], str]", value: Sequence, sep: str = " "):
    return "[{}]".format(("," + sep).join(f"{to_json(v, sep)}" for v in value))


def str_to_json(to_json, value: str, sep: str = " "):
    return '"{}"'.format(value.replace('"', r"\""))


def int_to_json(to_json, value: int, sep: str = " "):
    return str(value)


def float_to_json(to_json, value: float, sep: str = " "):
    if math.isnan(value):
        return "NaN"
    elif math.isinf(value):
        return "-Infinity" if value < 0 else "Infinity"
    else:
        return str(value)


def bool_to_json(to_json, value: bool, sep: str = " "):
    return "true" if value else "false"


def null_to_json(to_json, value: str, sep: str = " "):
    return "null"


def _isinstance(type_: type, value: Any) -> bool:
    return isinstance(value, type_)


def isinstance_(type_: type) -> Predicate[Any]:
    return partial(_isinstance, type_)


to_json: "StructuredRecursion[Any, [str], str]" = StructuredRecursion(
    [
        (isinstance_(str), str_to_json),
        (isinstance_(bool), bool_to_json),
        (isinstance_(int), int_to_json),
        (isinstance_(float), float_to_json),
        (isinstance_(type(None)), null_to_json),
        (isinstance_(Sequence), sequence_to_json),
        (isinstance_(Mapping), mapping_to_json),
    ],
    value_error("Don't know how to serialize {!r} to JSON"),
)


@pytest.mark.parametrize(
    "value",
    [
        "string",
        '"quotes"',
        1,
        123.4,
        float("inf"),
        float("-inf"),
        None,
        True,
        False,
        [],
        [1, 2, 3],
        [1, 2.3, "four", [5.6]],
        {},
        {"foo": 1, "bar": 2},
        {"foo": 1, "bar": 2.3, "baz": [4.5, {"6": 7}]},
    ],
)
def test_structured_recursion_json_roundtrip(value):
    json_str = to_json(value, " ")
    parsed = json.loads(json_str)
    assert parsed == value, value
