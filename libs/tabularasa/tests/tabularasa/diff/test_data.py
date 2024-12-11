import pandas as pd
import pytest

from thds.tabularasa.diff.data import DataFrameDiff


@pytest.mark.parametrize(
    "before_index, after_index, matched",
    [
        pytest.param(1, 1, 1, id="int matches"),
        pytest.param(1, "1", 0, id="int and string does not match"),
        pytest.param("1", "1", 1, id="string matches"),
        pytest.param(None, None, 1, id="None is None"),
        pytest.param(None, pd.NA, 0, id="None is not pandas.NA"),
        pytest.param(None, float("nan"), 0, id="None is not nan"),
        pytest.param(pd.NA, pd.NA, 1, id="pandas.NA is pandas.NA"),
        pytest.param(pd.NA, float("nan"), 0, id="pandas.NA is not nan"),
        pytest.param(float("nan"), float("nan"), 0, id="nan is not nan"),
        pytest.param([1, "1"], [1, "1"], 1, id="(int, str) matches (int, str)"),
        pytest.param([1, None], [1, None], 1, id="(int, None) matches (int, None)"),
        pytest.param([1, 1], [1, None], 0, id="(int, int) does not match (int, None)"),
        pytest.param([1, 1], [1, pd.NA], 0, id="(int, int) does not match (int, pandas.NA)"),
        pytest.param([1, 1], [1, float("nan")], 0, id="(int, int) does not match (int, nan)"),
        pytest.param([None, pd.NA], [None, pd.NA], 1, id="(None, pd.NA) matches (None, pd.NA"),
        pytest.param([float("nan")], [float("nan")], 1, id="nan in multi index matches"),
        pytest.param(
            [None, pd.NA, float("nan")],
            [None, pd.NA, float("nan")],
            1,
            id="all nulls are matched in multi index",
        ),
    ],
)
def test_common_keys(before_index, after_index, matched):
    """Test the method to find commond keys, given various keys, including None, pandas.NA, nan"""

    def _index(raw_index):
        if isinstance(raw_index, (tuple, list)):
            idx = pd.MultiIndex.from_tuples([tuple(raw_index)])
        else:
            idx = [raw_index]  # type: ignore
        return pd.DataFrame(index=idx)

    diff = DataFrameDiff(_index(before_index), _index(after_index))
    assert len(diff.common_keys) == matched
