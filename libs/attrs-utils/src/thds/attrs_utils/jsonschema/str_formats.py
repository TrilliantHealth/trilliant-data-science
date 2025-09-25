import datetime
import json
import re
import typing as ty
import uuid
from typing import Type, get_args

from ..recursion import RecF, value_error
from ..registry import Registry
from ..type_recursion import TypeRecursion
from .util import ToJSON

DATE, DATETIME, TIME, UUID = "date", "date-time", "time", "uuid"

_null_pattern = "^null$"
_str_pattern = r"^.*$"
_bool_pattern = r"^true|false$"
_int_pattern = r"^[+-]?[0-9]+$"
_float_pattern = r"^[-+]?([0-9]*\.?[0-9]+|[0-9]+)([eE][-+]?[0-9]+)?$"
_date_pattern = r"^[0-9]{4}-(0[1-9]|1[1-2])-(0[1-9]|1[0-9]|2[0-9]|3[0-1])$"
_time_pattern = r"^([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?([+-][0-5][0-9]:[0-5][0-9])?$"
_datetime_pattern = rf"^{_date_pattern[1:-1]}T{_time_pattern[1:-1]}$"
_uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

_uuid_re = re.compile(_uuid_pattern)

DEFAULT_FORMAT_CHECKS: ty.Dict[str, ty.Callable[[str], bool]] = {}


###################################################################
# String format checkers for use with jsonschema 'format' keyword #
###################################################################


def register_format_check(name: str):
    def decorator(func: ty.Callable[[str], bool]) -> ty.Callable[[str], bool]:
        DEFAULT_FORMAT_CHECKS[name] = func
        return func

    return decorator


def _is_datetime_string(s: str, cls: ty.Union[ty.Type[datetime.date], ty.Type[datetime.time]]) -> bool:
    try:
        cls.fromisoformat(s)
    except Exception:
        return False
    return True


@register_format_check(DATE)
def is_date_string(s: str) -> bool:
    return _is_datetime_string(s, datetime.date)


@register_format_check(DATETIME)
def is_datetime_string(s: str) -> bool:
    return _is_datetime_string(s, datetime.datetime)


@register_format_check(TIME)
def is_time_string(s: str) -> bool:
    return _is_datetime_string(s, datetime.time)


@register_format_check(UUID)
def is_uuid_string(s: str) -> bool:
    return isinstance(s, str) and bool(_uuid_re.fullmatch(s))


def string_pattern_for_literal(string_pattern_for, type_: Type, serializer: ToJSON) -> str:
    jsons = map(serializer, get_args(type_))

    def to_json_str(value):
        return value if isinstance(value, str) else json.dumps(value, indent="")

    strings = map(to_json_str, jsons)
    return rf"^({'|'.join(map(re.escape, strings))})$"


def string_pattern_for_union(string_pattern_for, type_: Type, serializer: ToJSON) -> str:
    patterns = (string_pattern_for(t, serializer) for t in get_args(type_))
    return rf"^({'|'.join(patterns)})$"


JSON_STRING_PATTERN_REGISTRY = Registry(
    {
        type(None): _null_pattern,
        bool: _bool_pattern,
        int: _int_pattern,
        float: _float_pattern,
        datetime.date: _date_pattern,
        datetime.datetime: _datetime_pattern,
        uuid.UUID: _uuid_pattern,
    }
)

unknown_type_for_json_str_pattern: "RecF[Type, [ToJSON], str]" = value_error(
    "Can't determine a format regex for type {!r} embedded as a string in json; "
    f"register one with {__name__}.string_pattern_for.register()",
    TypeError,
)

string_pattern_for: "TypeRecursion[[ToJSON], str]" = TypeRecursion(
    JSON_STRING_PATTERN_REGISTRY,
    literal=string_pattern_for_literal,
    union=string_pattern_for_union,
    otherwise=unknown_type_for_json_str_pattern,
)
