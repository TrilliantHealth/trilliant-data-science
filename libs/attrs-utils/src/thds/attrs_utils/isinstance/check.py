from typing import Any, NamedTuple, Type, cast, get_args, get_origin

import attrs

from .. import recursion, type_utils
from ..params import attrs_fields_parameterized
from ..type_recursion import TypeRecursion
from . import util
from .registry import ISINSTANCE_REGISTRY
from .util import Check

_UNKNOWN_TYPE = (
    "Don't know how to check for instances of {!r}; use "
    f"{__name__}.instancecheck.register to register an instance check"
)

unknown_type: "recursion.RecF[Type, [], Check]" = recursion.value_error(_UNKNOWN_TYPE, TypeError)


def simple_isinstance(instancecheck, type_: Type):
    return util.simple_isinstance(type_)


def check_literal(instancecheck, type_: Type):
    return util.check_in_values(get_args(type_))


def check_attrs(instancecheck, type_: Type[attrs.AttrsInstance]):
    # this _should_ typecheck according to my understanding of attrs.AttrsInstance but it is not
    fields = attrs_fields_parameterized(type_)
    names = tuple(f.name for f in fields)
    types = (f.type for f in fields)
    return util.check_attrs(
        util.simple_isinstance(type_),
        names,
        *map(instancecheck, types),
    )


def check_namedtuple(instancecheck, type_: Type[NamedTuple]):
    field_types = tuple(type_.__annotations__[name] for name in type_._fields)
    return util.check_typed_tuple(util.simple_isinstance(type_), *map(instancecheck, field_types))


def check_mapping(instancecheck, type_: Type):
    args = get_args(type_)
    if len(args) != 2:
        return unknown_type(instancecheck, type_)
    kt, vt = args
    k_check = instancecheck(kt)
    v_check = instancecheck(vt)
    org = cast(Type, get_origin(type_))
    return util.check_mapping(util.simple_isinstance(org), k_check, v_check)


def check_tuple(instancecheck, type_: Type):
    org = cast(Type, get_origin(type_))
    args = get_args(type_)
    return util.check_typed_tuple(util.simple_isinstance(org), *map(instancecheck, args))


def check_variadic_tuple(instancecheck, type_: Type):
    org = cast(Type, get_origin(type_))
    args = get_args(type_)
    v_check = instancecheck(args[0])
    return util.check_collection(util.simple_isinstance(org), v_check)


def check_collection(instancecheck, type_: Type):
    args = get_args(type_)
    if len(args) != 1:
        return unknown_type(instancecheck, type_)
    org = cast(Type, get_origin(type_))
    v_check = instancecheck(args[0])
    return util.check_collection(util.simple_isinstance(org), v_check)


def check_union(instancecheck, type_: Type):
    return util.check_any(*map(instancecheck, get_args(type_)))


def check_any(instancecheck, type_: Type):
    if isinstance(type_, type):
        # simple concrete type
        return util.simple_isinstance(type_)
    return unknown_type(instancecheck, type_)


instancecheck: "TypeRecursion[[], Check]" = TypeRecursion(
    ISINSTANCE_REGISTRY,
    # just use isinstance on builtins - don't want to check bytes/str as collections for instance.
    # also an optimization to put this check first since these are the most common types.
    first=(type_utils.is_builtin_type, simple_isinstance),
    literal=check_literal,
    enum=simple_isinstance,
    attrs=check_attrs,
    namedtuple=check_namedtuple,
    union=check_union,
    mapping=check_mapping,
    collection=check_collection,
    tuple=check_tuple,
    variadic_tuple=check_variadic_tuple,
    otherwise=check_any,
)


def isinstance(obj: Any, type_: Type) -> bool:
    return instancecheck(type_)(obj)
