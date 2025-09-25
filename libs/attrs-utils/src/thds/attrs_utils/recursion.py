from functools import partial
from typing import Any, Callable, Generic, Sequence, Tuple, Type, TypeVar

from typing_extensions import Concatenate, ParamSpec

T = TypeVar("T", contravariant=True)
U = TypeVar("U", covariant=True)
Params = ParamSpec("Params")

Predicate = Callable[[T], bool]
F = Callable[Concatenate[T, Params], U]
RecF = Callable[Concatenate[F, T, Params], U]


class StructuredRecursion(Generic[T, Params, U]):
    def __init__(
        self,
        guarded_recursions: Sequence[Tuple[Predicate[T], RecF[T, Params, U]]],
        fallback: RecF[T, Params, U],
    ):
        self.guarded_recursions = list(guarded_recursions)
        self.fallback = fallback

    def __call__(self, obj: T, *args: Params.args, **kwargs: Params.kwargs) -> U:
        for predicate, recurse in self.guarded_recursions:
            if predicate(obj):
                return recurse(self, obj, *args, **kwargs)
        else:
            return self.fallback(self, obj, *args, **kwargs)


def _value_error(
    msg: str,
    exc_type: Type[Exception],
    # placeholder for the recursive function in case you wish to specify this explictly as one of the
    # recursions; type doesn't matter
    f: Any,
    obj: T,
    *args,
    **kwargs,
):
    raise exc_type(msg.format(obj))


def value_error(msg: str, exc_type: Type[Exception] = ValueError) -> RecF[T, Params, U]:
    """Helper to be passed as the `fallback` of a `StructuredRecursion` in case there is no natural
    fallback/default implementation and an input fails to satisfy any of the predicates"""
    return partial(_value_error, msg, exc_type)
