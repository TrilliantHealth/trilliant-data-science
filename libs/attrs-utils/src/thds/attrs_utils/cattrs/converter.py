import datetime
import decimal
import typing as ty
from functools import partial
from typing import Any, Callable, Sequence, Tuple, Type

from cattr.converters import Converter, GenConverter

from ..type_utils import is_literal_type, is_set_type, literal_base
from . import errors

T = ty.TypeVar("T")
Struct = Callable[[Any, Type[T]], T]
UnStruct = Callable[[Any], T]
StructFactory = Callable[[Any], Struct[T]]


PREJSON_UNSTRUCTURE_COLLECTION_OVERRIDES: ty.Mapping[Type, Callable[[], ty.Collection]] = {
    ty.Set: list,
    ty.FrozenSet: list,
    ty.AbstractSet: list,
    ty.MutableSet: list,
    ty.Sequence: list,
    ty.DefaultDict: dict,
    ty.OrderedDict: dict,
    ty.Mapping: dict,
    ty.MutableMapping: dict,
}


# hooks


def _date_from_isoformat(dt: ty.Union[str, datetime.date], t: Type[datetime.date]) -> datetime.date:
    if isinstance(dt, datetime.datetime):  # check most specific type first
        return dt.date()
    if isinstance(dt, datetime.date):
        return dt
    return t.fromisoformat(str(dt))


def _datetime_from_isoformat(
    dt: ty.Union[str, datetime.date], t: Type[datetime.datetime]
) -> datetime.datetime:
    if isinstance(dt, datetime.datetime):
        return dt
    if isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day)
    return t.fromisoformat(str(dt))


def unstructure_set(converter: Converter, items: ty.AbstractSet) -> ty.List:
    sorted_items = sorted(items)
    return list(map(converter.unstructure, sorted_items))


def _structure_as(converter: Converter, actual_type: Type[T], obj: Any, _: Any) -> T:
    return converter.structure(obj, actual_type)


def structure_as(converter: Converter, actual_type: Type[T]) -> Struct[T]:
    """Define a custom structuring function for cattrs which works by deferring to another type.
    This is useful for some types which cattrs does not natively support, but which otherwise have implementations
    using types that it *does* support; e.g. NewType(str) or a Union thereof"""
    return partial(_structure_as, converter, actual_type)


def _structure_literal_as_base_type(converter: Converter, type_: Type) -> Struct:
    base_type = literal_base(type_)
    return structure_as(converter, base_type)


def structure_literal_as_base_type(converter: Converter) -> StructFactory:
    return partial(_structure_literal_as_base_type, converter)


def _structure_restricted_conversion(
    allowed_types: Tuple[Type, ...], parse: Callable[[Any], T], value: Any, _: Type[T]
) -> T:
    if type(value) in allowed_types:
        return parse(value)
    raise errors.DisallowedConversionError(type(value), _)


def structure_restricted_conversion(
    allowed_types: Tuple[Type, ...], parse: Callable[[Any], T]
) -> Struct[T]:
    """This hook ensures that the more dubious behaviors of cattrs are forbidden; only values which can
    be very unambiguously interpreted as the target type are allowed to be unstructured as such."""
    return partial(_structure_restricted_conversion, allowed_types, parse)


# default hooks

DEFAULT_STRUCTURE_HOOKS: Sequence[Tuple[Type, Struct]] = (
    (datetime.date, _date_from_isoformat),
    (datetime.datetime, _datetime_from_isoformat),
)
DEFAULT_UNSTRUCTURE_HOOKS_JSON: Sequence[Tuple[Type, UnStruct]] = (
    (datetime.date, datetime.date.isoformat),
    (datetime.datetime, datetime.datetime.isoformat),
    (decimal.Decimal, float),
)
DEFAULT_RESTRICTED_CONVERSIONS: Sequence[Tuple[Tuple[Type, ...], Type, UnStruct]] = (
    ((int, str), str, str),
    ((int, str, float, decimal.Decimal), float, float),
    ((bool, str, int), int, int),
    ((bool,), bool, bool),
)


# default converters


def setup_converter(
    converter: Converter,
    struct_hooks: Sequence[Tuple[Type[T], Struct[T]]] = DEFAULT_STRUCTURE_HOOKS,
    unstruct_hooks: Sequence[Tuple[Type[T], UnStruct[T]]] = (),
    custom_structure_as: Sequence[Tuple[Type, Type]] = (),
    restricted_conversions: Sequence[
        Tuple[Tuple[Type, ...], Type[T], UnStruct[T]]
    ] = DEFAULT_RESTRICTED_CONVERSIONS,
    deterministic: bool = True,
    strict_enums: bool = False,
) -> Converter:
    """Performs side effects on your converter, registering various hooks.

    :param converter: the cattrs Converter to update
    :param struct_hooks: custom structuring hooks as tuples of (type, callable(value, type) -> value)
    :param unstruct_hooks: custom unstructuring hooks as tuples of (type, callable(value) -> value)
    :param custom_structure_as: tuples of (type1, type2). When these are passed, the converter will
      structure values into type1 using the pre-defined behavior for type2. Useful for specifying that
      some complex type can really be treated in a simpler way (e.g. that a union over several string
      literal enums can just be structured as a string).
    :param restricted_conversions: tuples of ((in_type, ...), out_type, parser(value) -> out_type).
      When an input is to be structured into the out_type, it will only be allowed to do so if its
      concrete type is in the in_type tuple (strict inclusion, not `isinstance` check, to avoid confusion
      e.g. from subtle subclass relations such as bool being a subclass of int). The default includes a
      reasonable set of restrictions that are *not* by default respected by cattrs.
    :param deterministic: when True, an attempt is made to produce deterministic output on unstructuring.
      For example, sets will be sorted prior to being unstructured (but not after, since in general
      unstructured values such as dicts may not be orderable). This requires all types contained within
      sets in your data model to be orderable. Defaults to True.
    :param strict_enums: when True, on unstructuring into a Literal type, an error will be raised when
      the input value is not one of the expected values. This is the default behavior of cattrs as of
      version 22. You may not wish to be so strict when accepting data from a source with potential data
      quality issues, preferring perhaps to clean up any unexpected values after structuring, in which
      case this should be False, the default value.
    :return: the modified converter with hooks registered.
    """
    for target_type, struct in struct_hooks:
        converter.register_structure_hook(target_type, struct)
    for target_type, unstruct in unstruct_hooks:
        converter.register_unstructure_hook(target_type, unstruct)
    for input_types, target_type, parser in restricted_conversions:
        converter.register_structure_hook(
            target_type, structure_restricted_conversion(input_types, parser)
        )
    for actual_type, override_type in custom_structure_as:
        converter.register_structure_hook(actual_type, structure_as(converter, override_type))
    if deterministic:
        converter.register_unstructure_hook_func(is_set_type, partial(unstructure_set, converter))
    if not strict_enums:
        converter.register_structure_hook_factory(
            is_literal_type, structure_literal_as_base_type(converter)
        )
    return converter


def default_converter(
    *,
    forbid_extra_keys: bool = True,
    prefer_attrib_converters: bool = True,
    unstruct_collection_overrides: ty.Mapping[
        Type, Callable[[], ty.Collection]
    ] = PREJSON_UNSTRUCTURE_COLLECTION_OVERRIDES,
) -> Converter:
    return GenConverter(
        unstruct_collection_overrides=unstruct_collection_overrides,
        prefer_attrib_converters=prefer_attrib_converters,
        forbid_extra_keys=forbid_extra_keys,
    )
