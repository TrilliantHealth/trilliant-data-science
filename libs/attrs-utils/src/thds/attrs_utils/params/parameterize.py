"""Utilities for parameterizing generic types and analyzing parameterized generic types."""

import inspect
import itertools
import typing as ty

import typing_inspect as ti

from ..utils import signature_preserving_cache
from .utils import TypeOrVar, origin_params_and_args


@ty.overload
def parameterize(
    type_: ty.TypeVar,
    params: ty.Union[ty.Mapping[ty.TypeVar, TypeOrVar], ty.Type],
) -> TypeOrVar: ...


@ty.overload
def parameterize(
    type_: ty.Type,
    params: ty.Union[ty.Mapping[ty.TypeVar, TypeOrVar], ty.Type],
) -> ty.Type: ...


def parameterize(
    type_: TypeOrVar,
    params: ty.Union[ty.Mapping[ty.TypeVar, TypeOrVar], ty.Type],
) -> TypeOrVar:
    """Given a generic type (or type variable) `type_` and a mapping from type variables to concrete types, return a new
    type where the type variables have been substituted according to the mapping. If `params` is itself a parameterized
    generic type, extract its type parameters and arguments and use those to build the mapping. If `type_` is not
    generic, return it unchanged.

    Examples:
        >>> from typing import Generic, TypeVar, List, Dict
        >>> T = TypeVar('T')
        >>> U = TypeVar('U')
        >>> V = TypeVar('V')
        >>> parameterize(int, {T: str}) == int
        True
        >>> parameterize(T, {T: int}) == int
        True
        >>> parameterize(List[T], {T: str}) == List[str]
        True
        >>> parameterize(Dict[T, U], {T: int, U: str, V: float}) == Dict[int, str]
        True
        >>> class MyGeneric(Generic[T, U]): ...
        >>> parameterize(MyGeneric, MyGeneric[int, str]) == MyGeneric[int, str]
        True
        >>> parameterize(List[T], MyGeneric[int, str]) == List[int]
        True
    """
    if not isinstance(params, ty.Mapping):
        _, params_, args, _ = origin_params_and_args(params)
        return parameterize(type_, dict(zip(params_, args)))
    elif isinstance(type_, ty.TypeVar):
        return params.get(type_, type_)
    elif tparams := ti.get_parameters(type_):
        new_args = tuple(params.get(t, t) for t in tparams)
        return type_[new_args]
    else:
        return type_


@signature_preserving_cache
def parameterized_mro(type_: ty.Type) -> ty.Tuple[ty.Type, ...]:
    """All generic bases of a generic type, with type parameters substituted according to their specification in `type_`,
    in python method resolution order. In case `type_` is not generic, just return the standard MRO.
    """
    # property-based test idea:
    #   map(partial(parameterize, args), parameterized_bases(t)) == parameterized_bases(t[args])

    def inner(type_: ty.Type, visited: ty.Set[ty.Type]) -> ty.Iterator[ty.Type]:
        origin, params, args, parameterized = origin_params_and_args(type_)
        if origin not in visited and origin is not ty.Generic:
            mapping = dict(zip(params, args))
            yield type_ if parameterized else parameterize(origin, mapping)
            visited.add(origin)
            for base in ti.get_generic_bases(origin):
                if ti.get_origin(base) is not ty.Generic:
                    yield from inner(parameterize(base, mapping), visited)

    if not ti.is_generic_type(type_):
        return inspect.getmro(type_)
    else:
        mro = dict(zip(inspect.getmro(ti.get_origin(type_) or type_), itertools.count()))
        pmro = sorted(inner(type_, set()), key=lambda type_: mro[ti.get_origin(type_) or type_])
        return tuple(pmro)
