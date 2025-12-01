import collections
import enum
from functools import partial
from typing import DefaultDict, Iterable, NamedTuple, Tuple, Type, TypeVar, cast, get_args

import attr

from .. import recursion, type_recursion, type_utils
from ..params import attrs_fields_parameterized
from . import attrs, collection, optional, tuple, union
from .registry import GEN_REGISTRY
from .util import Gen, T, U, choice_gen, juxtapose_gen, repeat_gen

_UNKNOWN_TYPE = (
    "Don't know how to generate random instances of {!r}; use "
    f"{__name__}.random_gen.register to register a random generator"
)

unknown_type: "recursion.RecF[Type, [], Gen]" = recursion.value_error(_UNKNOWN_TYPE, TypeError)


def gen_literal(random_gen, type_: Type[T]) -> Gen[T]:
    values = cast(Tuple[T], get_args(type_))
    return choice_gen(values)


def gen_enum(random_gen, type_: Type[T]) -> Gen[T]:
    assert issubclass(type_, enum.Enum)
    return choice_gen(list(type_))


def gen_attrs(random_gen, type_: Type[attr.AttrsInstance]) -> Gen[attr.AttrsInstance]:
    fields = attrs_fields_parameterized(type_)
    kw_only_fields = [f for f in fields if f.kw_only]
    overrides = attrs.CUSTOM_ATTRS_BY_FIELD_REGISTRY.get(type_)

    def random_gen_(field: attr.Attribute):
        if overrides:
            return overrides.get(field.name) or random_gen(field.type)
        return random_gen(field.type)

    if kw_only_fields:
        pos_fields = [f for f in fields if not f.kw_only]
        arg_gens = list(map(random_gen_, pos_fields))
        kwarg_gens = {f.name: random_gen_(f) for f in kw_only_fields}
        return partial(attrs.random_attrs, type_, arg_gens, kwarg_gens)
    else:
        field_gens = map(random_gen_, fields)
        return tuple.random_namedtuple_gen(type_, *field_gens)


NT = TypeVar("NT", bound=NamedTuple)


def gen_namedtuple(random_gen, type_: Type[NT]) -> Gen[NT]:
    field_names = type_._fields
    field_types = (type_.__annotations__[name] for name in field_names)
    overrides = attrs.CUSTOM_ATTRS_BY_FIELD_REGISTRY.get(type_)
    if overrides:
        return tuple.random_namedtuple_gen(
            type_, *(overrides.get(name) or random_gen(t) for name, t in zip(field_types, field_names))
        )
    else:
        return tuple.random_namedtuple_gen(type_, *map(random_gen, field_types))


def gen_tuple(random_gen, type_: Type[T]) -> Gen[T]:
    args = get_args(type_)
    if not args:
        raise TypeError(_UNKNOWN_TYPE.format(type_))
    return cast(Gen[T], tuple.random_tuple_gen(*map(random_gen, args)))


def gen_variadic_tuple(random_gen, type_: Type[T]) -> Gen[T]:
    args = get_args(type_)
    cons = type_utils.concrete_constructor(type_)
    v_gen = random_gen(args[0])
    return cast(Gen[T], collection.random_collection_gen(cons, repeat_gen(v_gen)))


def gen_optional(random_gen, type_: Type[T]) -> Gen[T]:
    # more specialized that the union case just below
    return optional.random_optional_gen(
        random_gen(type_utils.unwrap_optional(type_))
    )  # type: ignore [return-value]


def gen_union(random_gen, type_: Type[T]) -> Gen[T]:
    return union.random_uniform_union_gen(*map(random_gen, get_args(type_)))


def _construct_defauldict(d: DefaultDict[T, U], kvs: Iterable[Tuple[T, U]]) -> DefaultDict[T, U]:
    for k, v in kvs:
        d[k] = v
    return d


def gen_mapping(random_gen, type_: Type[collection.M]) -> Gen[collection.M]:
    # more specific that collection case below
    args = get_args(type_)
    if len(args) != 2:
        raise TypeError(_UNKNOWN_TYPE.format(type_))
    kt, vt = args
    v_gen = random_gen(vt)
    kv_gen = juxtapose_gen(random_gen(kt), v_gen)

    cons = (
        partial(_construct_defauldict, collections.defaultdict(v_gen))
        if type_utils.get_origin(type_) is type_utils.get_origin(DefaultDict)
        else type_utils.concrete_constructor(type_)  # type: ignore [arg-type]
    )
    return collection.random_mapping_gen(cons, repeat_gen(kv_gen))  # type: ignore [arg-type]


def gen_collection(random_gen, type_: Type[collection.C]) -> Gen[collection.C]:
    # most generic collection case
    args = get_args(type_)
    if len(args) != 1:
        raise TypeError(_UNKNOWN_TYPE.format(type_))
    cons = type_utils.concrete_constructor(type_)
    v_gen = random_gen(args[0])
    return cast(Gen[collection.C], collection.random_collection_gen(cons, repeat_gen(v_gen)))


random_gen: "type_recursion.ConstructorFactory[[]]" = type_recursion.ConstructorFactory(
    GEN_REGISTRY,
    attrs=gen_attrs,
    namedtuple=gen_namedtuple,
    literal=gen_literal,
    enum=gen_enum,
    optional=gen_optional,
    union=gen_union,
    tuple=gen_tuple,
    variadic_tuple=gen_variadic_tuple,
    mapping=gen_mapping,
    collection=gen_collection,
    otherwise=unknown_type,
)
