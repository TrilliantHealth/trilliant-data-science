import typing as ty

import pandas as pd
import pytest

from thds.mllegos.feature_extraction.prefix_coding import optimize_prefix_coding


@pytest.mark.parametrize(
    "code_counts, min_count, expected_counts",
    [
        pytest.param(
            dict(
                a=10,
                ab=6,
                abc=4,
                abcd=3,
                abcde=2,
                abcdef=1,
            ),
            4,
            dict(a=10, ab=6, abc=4, abcd=6),
            id="linear-tree",
        ),
        pytest.param(
            dict(
                a=10,
                ab=6,
                abc=4,
                abcd=3,
                abcde=2,
                abcdef=1,
                _ab=3,  # this prefix is too short _but_ codes below it can aggregate up
                _abcd=3,
                _abcde=3,
                _abcdef=2,
            ),
            4,
            dict(a=10, ab=6, abc=4, abcd=6, _ab=6, _abcde=5),
            id="two-linear-trees-with-common-short-prefixes",
        ),
        pytest.param(
            dict(
                rar=3,
                rare=5,
                rarer=2,
                rarest=2,
                rareston=2,
                common=10,
                commoner=2,
                commonest=1,
                com=2,
            ),
            3,
            dict(rar=3, rare=7, rarest=4, common=10, commone=3),
            id="two-trees",
        ),
        pytest.param(
            dict(
                rar=3,
                rare=5,
                rarer=2,
                rarest=2,
                rareston=2,
                common=10,
                commoner=2,
                commonest=1,
                com=2,
                comm=1,  # these 2 roll up to 'com' but nothing else below them does; a "gap" in the hierarchy
            ),
            3,
            dict(rar=3, rare=7, rarest=4, com=3, common=10, commone=3),
            id="two-trees-with-hierarchy-gap",
        ),
    ],
)
def test_optimize_prefix_coding(
    code_counts: ty.Dict[str, int],
    min_count: int,
    expected_counts: ty.Dict[str, int],
):
    optimized_counts = optimize_prefix_coding(pd.Series(code_counts), min_count)
    assert optimized_counts == expected_counts
