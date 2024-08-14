from functools import partial
from typing import Callable, Sequence, Tuple, TypeVar

from .util import Gen, T

N = TypeVar("N")


def random_tuple(gens: Sequence[Gen[T]]) -> Tuple[T, ...]:
    # can't type this very well for heterogeneous tuples without going a little crazy.
    # most of the time it's being generated from types in an automated way so it's not a worry.
    return tuple(gen() for gen in gens)


def random_tuple_gen(*gens: Gen[T]) -> Gen[Tuple[T, ...]]:
    return partial(random_tuple, gens)


def random_namedtuple(constructor: Callable[..., N], gens: Sequence[Gen[T]]) -> N:
    return constructor(*random_tuple(gens))


def random_namedtuple_gen(constructor: Callable[..., N], *gens: Gen[T]) -> Gen[N]:
    return partial(random_namedtuple, constructor, gens)
