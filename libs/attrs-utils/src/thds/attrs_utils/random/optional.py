from functools import partial
from typing import Optional

from .builtin import random_bool
from .util import Gen, T


def random_optional(gen: Gen[T], nonnull: Gen[bool] = random_bool) -> Optional[T]:
    return gen() if nonnull() else None


def random_optional_gen(gen: Gen[T], nonnull: Gen[bool] = random_bool) -> Gen[Optional[T]]:
    return partial(random_optional, gen, nonnull)
