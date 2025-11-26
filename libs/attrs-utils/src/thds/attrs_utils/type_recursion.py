import dataclasses
import typing
from functools import partial
from typing import List, Optional, Tuple, Type, TypeVar

import attr
from typing_inspect import is_tuple_type, is_typevar, is_union_type

from . import type_utils
from .recursion import F, Params, Predicate, RecF, StructuredRecursion, U, _value_error
from .registry import Registry


def default_newtype(
    recurse: F[Type, Params, U], type_: Type, *args: Params.args, **kwargs: Params.kwargs
) -> U:
    """Default implementation for `typing.NewType` that recurses by simply applying the recursion to the
    ultimate concrete type underlying the newtype"""
    return recurse(type_utils.newtype_base(type_), *args, **kwargs)


def default_annotated(
    recurse: F[Type, Params, U], type_: Type, *args: Params.args, **kwargs: Params.kwargs
) -> U:
    """Default implementation for `typing.Annotated` that recurses by simply applying the recursion to
    the type that was wrapped with the annotation"""
    return recurse(type_utils.unwrap_annotated(type_), *args, **kwargs)


def _try_bases(
    msg: str,
    exc_type: Type[Exception],
    # placeholder for the recursive function in case you wish to specify this explictly as one of the
    # recursions; type doesn't matter
    f: F[Type, Params, U],
    type_: Type,
    *args: Params.args,
    **kwargs: Params.kwargs,
) -> U:
    try:
        mro = type_.mro()
    except (AttributeError, TypeError):
        return _value_error(msg, exc_type, f, type_, *args, **kwargs)
    for base in mro[1:]:
        try:
            return f(base, *args, **kwargs)
        except TypeError:
            pass
    return _value_error(msg, exc_type, f, type_, *args, **kwargs)


def try_bases(msg: str, exc_type: Type[Exception]) -> RecF[Type, Params, U]:
    """Helper to be passed as the `otherwise` of a `TypeRecursion` for handling cases of types which
    inherit from some known type that is not otherwise explicitly registered"""
    return partial(_try_bases, msg, exc_type)  # type: ignore[return-value]


class TypeRecursion(StructuredRecursion[Type, Params, U]):
    """Convenience class for defining functions which operate on runtime representations of types
    recursively. Handles the appropriate ordering of predicates such that more specific predicates
    precede more general ones. Also handles caching of results using the `registry` when `cached=True`,
    to allow for caching of results, since generally types are hashable never go out of program scope,
    and are not terribly numerous. With or without caching, the `registry` allows overriding with custom
    behavior for any specific type by using the `.register` method. Behavior registered this way will
    take precedence regardless of which predicates match the registered type."""

    def __init__(
        self,
        registry: Registry[Type, U],
        cached: bool = True,
        *,
        first: Optional[Tuple[Predicate[Type], RecF[Type, Params, U]]] = None,
        attrs: Optional[RecF[Type, Params, U]] = None,
        dataclass: Optional[RecF[Type, Params, U]] = None,
        namedtuple: Optional[RecF[Type, Params, U]] = None,
        optional: Optional[RecF[Type, Params, U]] = None,
        union: Optional[RecF[Type, Params, U]] = None,
        literal: Optional[RecF[Type, Params, U]] = None,
        enum: Optional[RecF[Type, Params, U]] = None,
        set: Optional[RecF[Type, Params, U]] = None,
        mapping: Optional[RecF[Type, Params, U]] = None,
        collection: Optional[RecF[Type, Params, U]] = None,
        tuple: Optional[RecF[Type, Params, U]] = None,
        variadic_tuple: Optional[RecF[Type, Params, U]] = None,
        newtype: Optional[RecF[Type, Params, U]] = default_newtype,  # type: ignore[assignment]
        annotated: Optional[RecF[Type, Params, U]] = default_annotated,  # type: ignore[assignment]
        typevar: Optional[RecF[Type, Params, U]] = None,
        otherwise: RecF[Type, Params, U],
    ):
        prioritized_funcs: List[Tuple[Predicate[Type], Optional[RecF[Type, Params, U]]]] = (
            [] if first is None else [first]
        )
        prioritized_funcs.extend(
            [
                (is_typevar, typevar),
                (type_utils.is_annotated_type, annotated),
                (type_utils.is_new_type, newtype),
                (type_utils.is_literal_type, literal),
                (type_utils.is_enum_type, enum),
                (attr.has, attrs),
                (dataclasses.is_dataclass, dataclass),
                (type_utils.is_optional_type, optional),
                (is_union_type, union),
                (type_utils.is_set_type, set),
                (type_utils.is_mapping_type, mapping),
                (type_utils.is_namedtuple_type, namedtuple),
                (type_utils.is_variadic_tuple_type, variadic_tuple),
                (is_tuple_type, tuple),
                (type_utils.is_collection_type, collection),
                (type_utils.is_annotated_type, annotated),
            ]
        )
        self.registry = registry
        self.cached = cached
        super().__init__(
            [(predicate, f) for predicate, f in prioritized_funcs if f is not None],
            otherwise,
        )

    def register(self, type_: Type, value: Optional[U] = None):
        """Convenience decorator factory"""
        if value is None:
            return self.registry.register(type_)
        else:
            return self.registry.register(type_, value)

    def __call__(self, obj: Type, *args: Params.args, **kwargs: Params.kwargs) -> U:
        if self.registry is None:
            return super().__call__(obj, *args, **kwargs)
        try:
            result = self.registry[obj]
        except KeyError:
            result = super().__call__(obj, *args, **kwargs)
            if self.cached:
                self.registry[obj] = result
        return result


T = TypeVar("T")
Constructor = typing.Callable[..., T]


class ConstructorFactory(TypeRecursion[Params, Constructor]):
    """Specialization of `TypeRecursion` for the common case of functions that take a type and return a function that
    returns instances of that type. Common examples include random or default instance generators.

    Consider the following example:

    ```python
    random_gen: TypeRecursion[[], Factory]]

    random_int = random_gen(int)  # returns a function that generates random integers
    random_str = random_gen(str)  # returns a function that generates random strings

    def add_one(x: int) -> int:
        return x + 1

    add_one(random_int())  # totally fine
    add_one(random_str())  # type error, but `mypy` doesn't catch it
    ```

    In this case, `add_one(random_str())` is a type error, but `mypy` doesn't catch it because `random_gen`'s signature
    does not constrain the returned constructor as returning the same type as the input type. If, on the other hand,
    `random_gen` had been typed as a `ConstructorFactory`, the type checker would be able to catch this error.
    It is up to you as the implementer to ensure that your implementation of any `ConstructorFactory` actually respects
    this constraint, since the implementation is too dynamic for most type checkers to verify this automatically, but if
    you do so, then the type checker _can_ detect common errors downstream of any given constructor creation.
    """

    def __call__(self, type_: Type[T], *args: Params.args, **kwargs: Params.kwargs) -> Constructor[T]:  # type: ignore[override]
        return super().__call__(type_)
