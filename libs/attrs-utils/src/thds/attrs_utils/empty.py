import dataclasses
import datetime
import enum
import inspect
import typing as ty
import uuid
import warnings
from functools import partial
from types import MappingProxyType

import attrs

from . import recursion, type_recursion, type_utils
from .params import attrs_fields_parameterized, dataclass_fields_parameterized
from .registry import Registry
from .type_recursion import Constructor

T = ty.TypeVar("T")


def const(value: T) -> Constructor[T]:
    return lambda: value


def empty_optional(empty_gen, type_: ty.Type[T]) -> Constructor[ty.Optional[T]]:
    return const(None)


def _construct_record(
    type_: ty.Type[T],
    signature: inspect.Signature,
    defaults: ty.Mapping[str, Constructor[ty.Any]],
    *args: ty.Any,
    **kwargs: ty.Any,
) -> T:
    bound = signature.bind_partial(*args, **kwargs)
    defaults_ = {k: v() for k, v in defaults.items() if k not in bound.arguments}
    bound.arguments.update(defaults_)
    return type_(*bound.args, **bound.kwargs)


def _record_constructor(
    type_: ty.Type[T],
    defaults: ty.Mapping[str, Constructor[ty.Any]],
    annotations: ty.Mapping[str, ty.Any] = MappingProxyType({}),
) -> Constructor[T]:
    constructible: ty.Type[T] = ty.get_origin(type_) or type_
    # ^ Introspect and construct via the unsubscripted origin of a parameterized generic (e.g. Foo[Bar] -> Foo).
    # inspect.signature(Foo[Bar]) reports a bare (*args, **kwargs) because it resolves through CPython's
    # typing._BaseGenericAlias.__call__ rather than the origin's __init__ - stripping the field names/defaults
    # we bind against. The origin's own signature is intact, and constructing it yields the same runtime object
    # (type params are erased at runtime). But the origin's annotations still carry raw type vars, so callers pass
    # `annotations` (resolved against the concrete params) to restore them in the documented signature.
    signature = inspect.signature(constructible)
    f = partial(
        _construct_record, constructible, signature, defaults
    )  # using partial application to allow picklability
    try:
        repl_signature = inspect.Signature(
            [
                p.replace(
                    default=defaults[p.name]() if p.name in defaults else p.default,
                    annotation=annotations.get(p.name, p.annotation),
                )
                for p in signature.parameters.values()
            ],
            return_annotation=signature.return_annotation,
        )
    except Exception as e:
        warnings.warn(
            f"Couldn't generate signature for {type_} with new defaults {defaults}; using original signature for documenation: {e}"
        )
        repl_signature = signature
    f.__signature__ = repl_signature  # type: ignore[attr-defined]
    # ^ allows tab completion in REPLs
    return f


def empty_attrs(empty_gen, type_: ty.Type[attrs.AttrsInstance]) -> Constructor[attrs.AttrsInstance]:
    fields = attrs_fields_parameterized(type_)
    defaults = {f.name: empty_gen(f.type) for f in fields if f.default is attrs.NOTHING}
    # keep the original defaults and default factories; fields carry types resolved against the concrete
    # params, so pass them as annotations to keep type vars out of the generated signature
    return _record_constructor(type_, defaults, {f.name: f.type for f in fields})


def empty_dataclass(empty_gen, type_: ty.Type[T]) -> Constructor[T]:
    fields = dataclass_fields_parameterized(type_)
    defaults = {
        f.name: empty_gen(f.type)
        for f in fields
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    }
    # a dataclass field is required only when it has neither a `default` nor a `default_factory` - unlike
    # attrs, which folds both into a single NOTHING sentinel. Otherwise this mirrors empty_attrs: keep the
    # field's own default/factory, and pass resolved types as annotations to keep type vars out of the signature.
    return _record_constructor(type_, defaults, {f.name: f.type for f in fields})


def empty_namedtuple(empty_gen, type_: ty.Type[ty.NamedTuple]) -> Constructor[ty.NamedTuple]:
    defaults = {
        name: empty_gen(t)
        for name, t in type_.__annotations__.items()
        if name not in type_._field_defaults
    }
    # keep the original defaults
    return _record_constructor(type_, defaults)


def empty_collection(empty_gen, type_: ty.Type[ty.Collection]) -> Constructor[ty.Collection]:
    return type_utils.concrete_constructor(type_)


def empty_literal(empty_gen, type_: ty.Type[T]) -> Constructor[T]:
    return const(ty.get_args(type_)[0])


def empty_enum(empty_gen, type_: ty.Type[enum.Enum]) -> Constructor[enum.Enum]:
    return const(next(iter(type_)))


def empty_union(empty_gen, type_: ty.Type[T]) -> Constructor[T]:
    return empty_gen(ty.get_args(type_)[0])  # construct the first type in the union


def _construct_tuple(
    type_: ty.Callable[..., ty.Tuple], defaults: ty.Sequence[Constructor[ty.Any]], *args: ty.Any
) -> ty.Tuple:
    return type_([*args, *(f() for f in defaults[len(args) :])])


def empty_tuple(empty_gen, type_: ty.Type[ty.Tuple]) -> Constructor[ty.Tuple]:
    args = ty.get_args(type_)
    base = type_utils.concrete_constructor(type_)
    defaults = [empty_gen(arg) for arg in args]
    return partial(_construct_tuple, base, defaults)  # using partial application to allow picklability


unknown_type: "recursion.RecF[ty.Type, [], Constructor]" = recursion.value_error(
    "Don't know how to generate an 'empty' value for type {!r}; "
    f"use {__name__}.empty_gen.register to register an empty generator",
    TypeError,
)

REGISTRY: Registry[ty.Type, Constructor] = Registry(
    [
        (type(None), const(None)),
        *[(type_, type_) for type_ in [int, bool, float, str, bytes, bytearray]],
        (datetime.date, const(datetime.date.min)),
        (datetime.datetime, const(datetime.datetime.min)),
        (uuid.UUID, uuid.uuid4),
    ]
)

empty_gen: "type_recursion.ConstructorFactory[[]]" = type_recursion.ConstructorFactory(
    REGISTRY,
    cached=True,
    attrs=empty_attrs,
    dataclass=empty_dataclass,
    namedtuple=empty_namedtuple,
    literal=empty_literal,
    enum=empty_enum,
    optional=empty_optional,
    union=empty_union,
    tuple=empty_tuple,
    variadic_tuple=empty_collection,
    mapping=empty_collection,
    collection=empty_collection,
    otherwise=unknown_type,
)
