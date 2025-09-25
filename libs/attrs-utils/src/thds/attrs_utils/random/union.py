import random
from functools import partial
from itertools import accumulate
from operator import add
from typing import Sequence, Tuple

from .util import Gen, T


def random_weighted_union(gens: Sequence[Gen[T]], cum_weights: Sequence[float]):
    """optimized to use the cum_weights arg of random.choices"""
    gen = random.choices(gens, cum_weights=cum_weights, k=1)[0]
    return gen()


def random_weighted_union_gen(*weighted_gens: Tuple[Gen[T], float]) -> Gen[T]:
    # can't type this very well for heterogeneous tuples without going a little crazy.
    # most of the time it's being generated from types in an automated way so it's not a worry.
    gens, weights = zip(*weighted_gens)
    total = sum(weights)
    cum_weights = tuple(w / total for w in accumulate(weights, add))
    return partial(random_weighted_union, gens, cum_weights)


def random_uniform_union(gens: Sequence[Gen[T]]) -> T:
    gen = random.choice(gens)
    return gen()


def random_uniform_union_gen(*gens: Gen[T]) -> Gen[T]:
    # can't type this very well for heterogeneous tuples without going a little crazy.
    # most of the time it's being generated from types in an automated way so it's not a worry.
    return partial(random_uniform_union, gens)
