from itertools import chain
from typing import Dict

import numpy as np
import pandas as pd
import pytest

from thds.mllegos.sklegos.sampling import stratified

SINGLETONS = dict(a=1, b=1, c=1, d=1, e=1)  # N = 5
EQUAL = dict(a=10, b=10, c=10)  # N = 30
LINEAR = dict(a=5, b=10, c=15)  # N = 30
EXPONENTIAL = dict(a=1, b=1, c=2, d=4, e=8, f=16)  # N = 32
LONG_TAIL = dict(a=1, b=1, c=1, d=1, e=10, f=15, g=20, h=25)  # N = 74

SINGLETONS_N = sum(SINGLETONS.values())
EQUAL_N = sum(EQUAL.values())
LINEAR_N = sum(LINEAR.values())
EXPONENTIAL_N = sum(EXPONENTIAL.values())
LONG_TAIL_N = sum(LONG_TAIL.values())


@pytest.mark.parametrize(
    "counts, size, expected_counts",
    [
        pytest.param(SINGLETONS, SINGLETONS_N, SINGLETONS, id="singletons-full"),
        pytest.param(SINGLETONS, SINGLETONS_N // 2, SINGLETONS, id="singletons-half"),
        pytest.param(EQUAL, EQUAL_N, EQUAL, id="equal-full"),
        pytest.param(EQUAL, len(EQUAL), {k: 1 for k in EQUAL}, id="equal-one-per-class"),
        pytest.param(EQUAL, 2 * len(EQUAL), {k: 2 for k in EQUAL}, id="equal-two-per-class"),
        pytest.param(LINEAR, LINEAR_N, LINEAR, id="linear-full"),
        pytest.param(LINEAR, len(LINEAR), dict(a=1, b=1, c=1), id="linear-one-per-class"),
        pytest.param(LINEAR, LINEAR_N // 2, dict(a=3, b=5, c=7), id="linear-half"),
        pytest.param(EXPONENTIAL, EXPONENTIAL_N, EXPONENTIAL, id="exponential-full"),
        pytest.param(
            EXPONENTIAL,
            len(EXPONENTIAL),
            dict(a=1, b=1, c=1, d=1, e=1, f=3),
            id="exponential-one-per-class",
        ),
        pytest.param(
            EXPONENTIAL,
            EXPONENTIAL_N // 2,
            {k: v // 2 if v > 1 else 1 for k, v in EXPONENTIAL.items()},
            id="exponential-half",
        ),
        pytest.param(LONG_TAIL, LONG_TAIL_N, LONG_TAIL, id="long-tail-full"),
        pytest.param(
            LONG_TAIL,
            len(LONG_TAIL),
            dict(a=1, b=1, c=1, d=1, e=1, f=1, g=2, h=2),
            id="long-tail-one-per-class",
        ),
        pytest.param(
            LONG_TAIL,
            LONG_TAIL_N // 2,
            dict(a=1, b=1, c=1, d=1, e=5, f=7, g=10, h=12),
            id="long-tail-half",
        ),
    ],
)
def test_stratified_sample_ixs(
    counts: Dict[str, int],
    size: int,
    expected_counts: Dict[str, int],
):
    arr = np.array(list(chain.from_iterable([k] * n for k, n in counts.items())))
    series = pd.Series(arr, index=arr)
    frac = size / sum(counts.values())

    assert sum(expected_counts.values()) < size + len(counts)
    for array in (arr, series):
        for size_or_frac in frac, size:
            ixs = stratified.stratified_sample_ixs(array, size_or_frac)
            values = (array.iloc if isinstance(array, pd.Series) else array)[ixs]
            actual_counts = pd.Series(values).value_counts().to_dict()
            assert actual_counts == expected_counts
