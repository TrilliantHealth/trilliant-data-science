import itertools
from functools import partial
from typing import Callable, Collection, Iterable, Mapping, Tuple, TypeVar, cast

from .builtin import random_int_gen
from .util import Gen

T = TypeVar("T")
U = TypeVar("U")
C = TypeVar("C", bound=Collection)
M = TypeVar("M", bound=Mapping)
KV = Tuple[T, U]

COLLECTION_MIN_LEN = 0
COLLECTION_MAX_LEN = 32

default_len_gen = random_int_gen(COLLECTION_MIN_LEN, COLLECTION_MAX_LEN)


def random_collection(
    constructor: Callable[[Iterable[T]], C],
    value_gen: Gen[Iterable[T]],
    len_gen: Gen[int] = default_len_gen,
) -> C:
    return constructor(itertools.islice(value_gen(), len_gen()))


def random_collection_gen(
    constructor: Callable[[Iterable[T]], C],
    value_gen: Gen[Iterable[T]],
    len_gen: Gen[int] = default_len_gen,
) -> Gen[C]:
    return partial(random_collection, constructor, value_gen, len_gen)


def random_mapping_gen(
    constructor: Callable[[Iterable[KV]], M],
    kv_gen: Gen[Iterable[KV]],
    len_gen: Gen[int] = default_len_gen,
) -> Gen[M]:
    return cast(Gen[M], partial(random_collection, constructor, kv_gen, len_gen))
