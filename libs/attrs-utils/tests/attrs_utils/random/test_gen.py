import random
import typing as ty

import pytest

from thds.attrs_utils.isinstance import instancecheck
from thds.attrs_utils.random.gen import T, random_gen

from .. import conftest


@pytest.fixture
def random_seed():
    random.seed(1729)


@pytest.mark.parametrize(
    "type",
    conftest.TEST_TYPES,
)
def test_random_gen(type: ty.Type[T]):
    # check that a generator can be constructed
    gen = random_gen(type)
    check = instancecheck(type)

    for _ in range(10):
        # check that it can be called
        value = gen()

        assert check(value)
