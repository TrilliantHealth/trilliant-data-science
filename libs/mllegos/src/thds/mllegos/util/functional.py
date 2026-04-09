import functools
import typing as ty

import pandas as pd

from thds.core.log import getLogger

A = ty.TypeVar("A")
B = ty.TypeVar("B")
T = ty.TypeVar("T")

_LOGGER = getLogger(__name__)


def not_null(value: ty.Optional[T]) -> ty.TypeGuard[T]:
    return value is not None


def _list_map(f: ty.Callable[[T], T], values: ty.Iterable[T]) -> ty.List[T]:
    return list(map(f, values))


def list_map(f: ty.Callable[[T], T]) -> ty.Callable[[ty.Iterable[T]], ty.List[T]]:
    """Create a function to map a list of values using the given function."""
    return functools.partial(_list_map, f)


def _list_map_filter_null(f: ty.Callable[[T], ty.Optional[T]], values: ty.Iterable[T]) -> ty.List[T]:
    return list(filter(not_null, map(f, values)))  # type: ignore[arg-type]


def list_map_filter_null(
    f: ty.Callable[[T], ty.Optional[T]],
) -> ty.Callable[[ty.Iterable[T]], ty.List[T]]:
    """Create a function to map a list of values using the given function, filtering null results."""
    return functools.partial(_list_map_filter_null, f)


def _compose(f1: ty.Callable[[B], T], f2: ty.Callable[[A], B], value: A) -> T:
    # right-to-left
    return f1(f2(value))


def pipe(f1: ty.Callable[[A], B], f2: ty.Callable[[B], T]) -> ty.Callable[[A], T]:
    """Compose two functions, left-to-right."""
    return functools.partial(_compose, f2, f1)


def _allow_na(f: ty.Callable[[A], B], value: ty.Optional[A]) -> ty.Optional[B]:
    if pd.isna(value):  # type: ignore[arg-type]
        return None
    return f(value)


def allow_na(f: ty.Callable[[A], T]) -> ty.Callable[[ty.Optional[A]], ty.Optional[T]]:
    """Create a function that applies the given function to a value, returning None if the value is null
    (including all values considered null by pandas for convenience)."""
    return functools.partial(_allow_na, f)
