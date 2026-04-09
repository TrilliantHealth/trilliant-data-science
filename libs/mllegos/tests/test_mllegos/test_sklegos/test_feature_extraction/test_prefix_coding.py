import itertools
import random
from copy import copy

import pandas as pd
import pytest

from thds.mllegos.sklegos.feature_extraction.prefix_coding import OptimizedPrefixEncoding

TOKEN_COUNTS = dict(
    rar=3,
    rare=5,
    rarer=2,
    rarest=2,
    rareston=2,
    common=10,
    commoner=2,
    commonest=1,
    com=2,
    comm=1,
)
EXPECTED_TOKEN_COUNTS = dict(rar=3, rare=7, rarest=4, common=10, commone=3, com=3)
UNK = "<UNK>"
NA = "<NA>"


def random_case(s: str) -> str:
    return "".join(c.upper() if random.choice([True, False]) else c.lower() for c in s)


@pytest.fixture(scope="module")
def prefix_encoder():
    values = list(
        itertools.chain.from_iterable(
            map(random_case, itertools.repeat(k, v)) for k, v in TOKEN_COUNTS.items()
        )
    )
    X = pd.Series(values)
    transform = OptimizedPrefixEncoding(
        min_count=3, unknown_value=UNK, null_value=NA, normalizer=str.lower
    )
    transform.fit(X)
    assert transform.prefix_counts_ == EXPECTED_TOKEN_COUNTS
    return transform


@pytest.mark.parametrize(
    "X, array_valued, expected_encoded",
    [
        ([None], False, [NA]),
        (
            [
                "r",
                "RA",
                "Rar",
                "RaRe",
                "RARELY",
                "RareStone",
                "C",
                "co",
                "coM",
                "CoMm",
                "commely",
                "Commoneer",
                "commonStone",
                pd.NA,
                None,
            ],
            False,
            [
                UNK,
                UNK,
                "rar",
                "rare",
                "rare",
                "rarest",
                UNK,
                UNK,
                "com",
                "com",
                "com",
                "commone",
                "common",
                NA,
                NA,
            ],
        ),
        (
            [None, []],
            True,
            [None, []],
        ),
        (
            [None, [None], ["foobar", "RaRe"], ["CoMmOner", None, "comeon"], ["RAD"]],
            True,
            [None, [NA], [UNK, "rare"], ["commone", NA, "com"], [UNK]],
        ),
    ],
)
def test_optimized_prefix_encoding(prefix_encoder, X: list, array_valued: bool, expected_encoded: list):
    transformer = copy(prefix_encoder)
    transformer.array_valued = array_valued
    encoded = transformer.transform(pd.Series(X))
    assert isinstance(encoded, pd.Series)
    assert encoded.shape == (len(X),)
    pd.testing.assert_series_equal(encoded, pd.Series(expected_encoded))
