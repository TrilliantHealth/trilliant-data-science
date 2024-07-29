import sys
import typing as ty
from dataclasses import dataclass

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

P = ParamSpec("P")
R = ty.TypeVar("R")


@dataclass
class Thunk(ty.Generic[R]):
    """Result-typed callable with arguments partially applied beforehand."""

    func: ty.Callable
    args: P.args
    kwargs: P.kwargs

    def __init__(self, func: ty.Callable[P, R], *args: P.args, **kwargs: P.kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self) -> R:
        return ty.cast(R, self.func(*self.args, **self.kwargs))


def thunking(func: ty.Callable[P, R]) -> ty.Callable[P, Thunk[R]]:
    """Converts a standard function into a function that accepts the
    exact same arguments but returns a Thunk - something ready to be
    executed but the execution itself is deferred.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Thunk[R]:
        return Thunk(func, *args, **kwargs)

    return wrapper
