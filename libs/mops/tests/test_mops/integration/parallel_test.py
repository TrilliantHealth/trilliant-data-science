from functools import partial
from typing import List

import pytest

from thds.mops.parallel import parallel_yield_results


class TeensError(Exception):
    pass


class TwentiesError(Exception):
    pass


def test_parallel_yield_results():
    def whatever(i: int) -> int:
        if i > 20:
            raise TwentiesError(i)
        if i > 10:
            raise TeensError(i)
        return i

    results: List[int] = list()
    with pytest.raises(TeensError):
        for res in parallel_yield_results([partial(whatever, i) for i in range(27)]):  # type: ignore
            results.append(res)

    assert sorted(results) == list(range(11))
