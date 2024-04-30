from typing import Type

import pandas as pd
import pytest

from thds.tabularasa.data_dependencies.util import check_categorical_values


@pytest.mark.parametrize(
    "series, dtype, exc_type",
    [
        pytest.param(
            pd.Series([1, 2, 3, None], dtype=pd.Int16Dtype()),
            pd.CategoricalDtype([1, 2, 3, 4]),
            None,
            id="nullable int values vs int dtype w/ extra values",
        ),
        pytest.param(
            pd.Series([1, 2, 3, 4]),
            pd.CategoricalDtype([1, 2, 3]),
            ValueError,
            id="int values with invalid value vs int dtype",
        ),
        pytest.param(
            pd.Series([1, 2, 3, 4], dtype=pd.CategoricalDtype([5, 4, 3, 2, 1], ordered=True)),
            pd.CategoricalDtype([0, 1, 2, 3, 4], ordered=True),
            None,
            id="valid int values w/ int dtype vs int dtype, extra categories both sides",
        ),
        pytest.param(
            pd.Series([1, 2, 3, 4]),
            pd.CategoricalDtype(["1", "2", "3", "4"]),
            TypeError,
            id="int values vs string dtype with same ints cast",
        ),
        pytest.param(
            pd.Series(["1", "2", "3"]),
            pd.CategoricalDtype(["5", "4", "3", "2", "1"], ordered=True),
            None,
            id="string values vs string dtype with extra categories and ordering",
        ),
        pytest.param(
            pd.Series(
                ["1", "2", "3", None], dtype=pd.CategoricalDtype(["1", "2", "3", "4"], ordered=True)
            ),
            pd.CategoricalDtype(["1", "2", "3"]),
            None,
            id="valid string values with string dtype w/ extra categories vs string dtype",
        ),
        pytest.param(
            pd.Series(["1", "2", "3", None]),
            pd.CategoricalDtype(["0", "1", "2"]),
            ValueError,
            id="string values with invalid values vs string dtype",
        ),
        pytest.param(
            pd.Series([1, 2, 3, 5, None], dtype=pd.Int8Dtype()),
            pd.CategoricalDtype([1, 2, 3, 4]),
            ValueError,
            id="nullable string values with invalid value vs string dtype",
        ),
    ],
)
def test_check_categorical_values(
    series: pd.Series, dtype: pd.CategoricalDtype, exc_type: Type[Exception]
):
    if exc_type is None:
        check_categorical_values(series, dtype)
    else:
        with pytest.raises(exc_type):
            check_categorical_values(series, dtype)
