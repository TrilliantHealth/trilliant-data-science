from functools import partial
from typing import List

import pytest

from thds.core.parallel import IteratorWithLen, yield_results


def test_iterator_with_len():
    a = IteratorWithLen(5, range(5))
    b = IteratorWithLen(3, [5, 6, 7])

    assert len(a) == 5
    assert len(b) == 3
    ab = IteratorWithLen.chain(a, b)
    assert len(ab) == 8
    assert list(range(8)) == list(ab)


def test_can_compose_with_sequence():
    a = IteratorWithLen(5, range(5))
    b = (5, 6, 7)

    assert list(range(8)) == list(IteratorWithLen.chain(a, b))


class TeensError(Exception):
    pass


class TwentiesError(Exception):
    pass


def test_parallel_yield_results(caplog):
    def whatever(i: int) -> int:
        if i > 20:
            raise TwentiesError(i)
        if i > 10:
            raise TeensError(i)
        return i

    results: List[int] = list()
    with pytest.raises(TeensError):
        for res in yield_results([partial(whatever, i) for i in reversed(range(27))]):  # type: ignore
            results.append(res)

    assert sorted(results) == list(range(11))
