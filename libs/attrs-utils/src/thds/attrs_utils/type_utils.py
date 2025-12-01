import collections
import enum
import inspect
import typing
from typing import Callable, List, Optional, Tuple, Type, TypeVar, Union

import attr
from typing_extensions import TypeIs
from typing_inspect import (
    get_args,
    get_origin,
    is_literal_type,
    is_new_type,
    is_optional_type,
    is_tuple_type,
    is_typevar,
)

T = TypeVar("T")

COLLECTION_TYPES = set(
    map(
        get_origin,
        [
            typing.List,
            typing.Tuple,
            typing.Set,
            typing.MutableSet,
            typing.FrozenSet,
            typing.AbstractSet,
            typing.Sequence,
            typing.Collection,
        ],
    )
)
UNIQUE_COLLECTION_TYPES = set(
    map(get_origin, [typing.Set, typing.MutableSet, typing.FrozenSet, typing.AbstractSet])
)
MAPPING_TYPES = set(
    map(
        get_origin,
        [
            typing.Dict,
            typing.Mapping,
            typing.MutableMapping,
            typing.DefaultDict,
            typing.OrderedDict,
            typing.Counter,
        ],
    )
)
TUPLE = get_origin(Tuple)
ORIGIN_TO_CONSTRUCTOR = {
    get_origin(t): c
    for t, c in [
        (typing.List, list),
        (typing.Tuple, tuple),
        (typing.Set, set),
        (typing.MutableSet, set),
        (typing.AbstractSet, set),
        (typing.FrozenSet, frozenset),
        (typing.Sequence, list),
        (typing.Collection, list),
        (typing.Dict, dict),
        (typing.Mapping, dict),
        (typing.MutableMapping, dict),
        (typing.OrderedDict, collections.OrderedDict),
    ]
}


def typename(type_: Type) -> str:
    if attr.has(type_) or is_new_type(type_) or is_typevar(type_):
        return type_.__name__
    else:
        raise TypeError(f"can't generate meaningful name for type {type_}")


def newtype_base(type_: Type) -> Type:
    return newtype_base(type_.__supertype__) if is_new_type(type_) else type_


def literal_base(type_: Type) -> Type:
    assert is_literal_type(type_)
    types = tuple(map(type, get_args(type_)))
    return Union[types]  # type: ignore


def enum_base(type_: Type) -> Type:
    assert is_enum_type(type_)
    if issubclass(type_, int):  # IntEnum, IntFlag
        return int
    types = tuple(type(e.value) for e in type_)
    return Union[types]  # type: ignore


def unwrap_optional(type_: Type) -> Type:
    if is_optional_type(type_):
        args = get_args(type_)
        return Union[tuple(a for a in args if a is not type(None))]  # type: ignore[return-value]  # noqa:E721
    else:
        return type_


def is_annotated_type(type_: Type) -> bool:
    return (
        hasattr(type_, "__origin__")
        and hasattr(type_, "__args__")
        and isinstance(getattr(type_, "__metadata__", None), tuple)
    )


def unwrap_annotated(type_: Type) -> Type:
    if is_annotated_type(type_):
        return type_.__origin__
    return type_


def is_enum_type(type_: Type) -> TypeIs[Type[enum.Enum]]:
    return isinstance(type_, type) and issubclass(type_, enum.Enum)


def is_collection_type(type_: Type) -> TypeIs[Type[typing.Collection]]:
    origin = get_origin(type_)
    return (type_ in COLLECTION_TYPES) if origin is None else (origin in COLLECTION_TYPES)


def is_mapping_type(type_: Type) -> TypeIs[Type[typing.Mapping]]:
    origin = get_origin(type_)
    return (type_ in MAPPING_TYPES) if origin is None else (origin in MAPPING_TYPES)


def is_set_type(type_: Type) -> TypeIs[Type[typing.AbstractSet]]:
    origin = get_origin(type_)
    return (type_ in UNIQUE_COLLECTION_TYPES) if origin is None else (origin in UNIQUE_COLLECTION_TYPES)


def is_namedtuple_type(type_: Type) -> TypeIs[Type[typing.NamedTuple]]:
    return getattr(type_, "__bases__", None) == (tuple,) and hasattr(type_, "_fields")


def is_variadic_tuple_type(type_: Type) -> TypeIs[Type[typing.Tuple[typing.Any, ...]]]:
    if is_tuple_type(type_):
        args = get_args(type_)
        return len(args) == 2 and args[-1] is Ellipsis
    else:
        return False


def is_builtin_type(type_: Type) -> bool:
    return getattr(type_, "__module__", None) == "builtins" and not get_args(type_)


def concrete_constructor(type_: Type[T]) -> Callable[..., T]:
    if is_namedtuple_type(type_):
        return type_  # type: ignore[return-value]
    origin = get_origin(type_)
    return ORIGIN_TO_CONSTRUCTOR[type_] if origin is None else ORIGIN_TO_CONSTRUCTOR[origin]


def bases(type_: Type, predicate: Optional[Callable[[Type], bool]] = None) -> List[Type]:
    if get_args(type_):
        type_ = get_origin(type_)
    if not inspect.isclass(type_):
        raise TypeError(
            f"{bases.__module__}.{bases.__name__} can be called only on concrete classes; got {type_}"
        )
    elif predicate is None:
        return list(inspect.getmro(type_))
    else:
        return list(filter(predicate, inspect.getmro(type_)))
