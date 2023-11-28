from typing import List, Optional, Tuple, Type

import attr
from typing_inspect import is_tuple_type, is_typevar, is_union_type

from . import type_utils
from .recursion import F, Params, Predicate, RecF, StructuredRecursion, U
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
        namedtuple: Optional[RecF[Type, Params, U]] = None,
        optional: Optional[RecF[Type, Params, U]] = None,
        union: Optional[RecF[Type, Params, U]] = None,
        literal: Optional[RecF[Type, Params, U]] = None,
        set: Optional[RecF[Type, Params, U]] = None,
        mapping: Optional[RecF[Type, Params, U]] = None,
        collection: Optional[RecF[Type, Params, U]] = None,
        tuple: Optional[RecF[Type, Params, U]] = None,
        variadic_tuple: Optional[RecF[Type, Params, U]] = None,
        newtype: Optional[RecF[Type, Params, U]] = default_newtype,
        annotated: Optional[RecF[Type, Params, U]] = default_annotated,
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
                (attr.has, attrs),
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
