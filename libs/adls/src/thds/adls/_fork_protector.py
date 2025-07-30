import os
import typing as ty
from functools import partial

from typing_extensions import Concatenate, ParamSpec

P = ParamSpec("P")
T = ty.TypeVar("T")
F = ty.TypeVar("F", bound=ty.Callable)


def _pid_swallower(
    func: ty.Callable[P, T],
    pid: int,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    return func(*args, **kwargs)


def _pid_sending_wrapper(
    caching_pid_swallower: ty.Callable[Concatenate[int, P], T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    return caching_pid_swallower(os.getpid(), *args, **kwargs)


def fork_safe_cached(
    cache_deco: ty.Callable[[F], F],
    func: ty.Callable[P, T],
) -> ty.Callable[P, T]:
    """Decorator to make a fork-safe cached.locking function by wrapping it in a function that
    always calls os.getpid() to invalidate the cache on new processes."""
    return partial(
        _pid_sending_wrapper,
        cache_deco(ty.cast(F, partial(_pid_swallower, func))),
    )
