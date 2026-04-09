import numpy as np
import pandas as pd
import pytest
from scipy.sparse import coo_matrix, csr_matrix

from thds.mllegos.sklegos import array_compat, types


@pytest.mark.parametrize(
    "arr, expected",
    [
        (np.array([1, 2, 3]), np.array([1, 2, 3])),
        (pd.Series([1, 2, 3]), np.array([1, 2, 3])),
        (
            pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}),
            np.array([[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]]),
        ),
        (csr_matrix([[1, 2, 3], [0, 0, 0]]), np.array([[1, 2, 3], [0, 0, 0]])),
        (coo_matrix([[1, 2, 3], [0, 0, 0]]), np.array([[1, 2, 3], [0, 0, 0]])),
    ],
)
def test_to_np(arr: types.Any2DArray, expected: np.ndarray):
    assert np.array_equal(array_compat.to_np(arr), expected)


DF = pd.DataFrame(dict(a=[0, 1, 2, 3], b=[4, 5, 6, 7], c=[0, 0, 0, 0]))
NP = DF.values
SP = csr_matrix(NP)


@pytest.mark.parametrize(
    "arr, row, col, expected",
    [
        (DF, None, None, DF),
        (DF, slice(1, 3), None, DF.iloc[1:3]),
        (DF, [0, 2], None, DF.iloc[[0, 2]]),
        (DF, None, slice(1, 3), DF.iloc[:, 1:3]),
        (DF, None, [1, 2], DF.iloc[:, [1, 2]]),
        (DF, slice(1, 3), slice(1, 3), DF.iloc[1:3, 1:3]),
        (DF, [0, 2], [1, 2], DF.iloc[[0, 2], [1, 2]]),
        (DF, np.array([False, True, False, True]), None, DF.iloc[[1, 3]]),
        (DF, None, np.array([False, True, True]), DF.iloc[:, [1, 2]]),
        (
            DF,
            np.array([False, True, False, True]),
            np.array([False, True, True]),
            DF.iloc[[1, 3], [1, 2]],
        ),
        (DF, 0, [0, 1], DF.iloc[0, [0, 1]]),
        (DF, np.array([False, True, False, True]), 0, DF.iloc[[1, 3], 0]),
        (NP, None, None, NP),
        (NP, slice(1, 3), None, NP[1:3]),
        (NP, [1, 3], None, NP[[1, 3], :]),
        (NP, None, slice(1, 3), NP[:, 1:3]),
        (NP, None, [0, 2], NP[:, [0, 2]]),
        (NP, slice(1, 3), slice(1, 3), NP[1:3, 1:3]),
        (NP, [1, 3], [0, 2], NP[[1, 3]][:, [0, 2]]),
        (NP, np.array([False, True, False, True]), None, NP[[1, 3]]),
        (NP, None, np.array([False, True, True]), NP[:, [1, 2]]),
        (NP, np.array([False, True, False, True]), np.array([False, True, True]), NP[[1, 3]][:, [1, 2]]),
        (NP, 0, [0, 1], NP[0, [0, 1]]),
        (NP, np.array([False, True, False, True]), 0, NP[[1, 3], 0]),
        (SP, None, None, SP),
        (SP, slice(1, 3), None, SP[1:3]),
        (SP, [1, 3], None, SP[[1, 3], :]),
        (SP, None, slice(1, 3), SP[:, 1:3]),
        (SP, None, [0, 2], SP[:, [0, 2]]),
        (SP, slice(1, 3), slice(1, 3), SP[1:3, 1:3]),
        (SP, [1, 3], [0, 2], SP[[1, 3]][:, [0, 2]]),
        (SP, np.array([False, True, False, True]), None, SP[[1, 3]]),
        (SP, None, np.array([False, True, True]), SP[:, [1, 2]]),
        (SP, np.array([False, True, False, True]), np.array([False, True, True]), SP[[1, 3]][:, [1, 2]]),
        (SP, 0, [0, 1], SP[0, [0, 1]]),
        (SP, np.array([False, True, False, True]), 0, SP[[1, 3], 0]),
    ],
)
def test_slice_2d(
    arr: types.Any2DArray,
    row: array_compat.AnyIndexer,
    col: array_compat.AnyIndexer,
    expected: types.Any2DArray,
):
    sliced = array_compat.slice_2d(arr, row, col)
    assert type(sliced) is type(expected)
    assert sliced.shape == expected.shape
    if (isinstance(row, int) or isinstance(col, int)) and not isinstance(arr, (csr_matrix, coo_matrix)):
        # slicing a single row or column from a dense array will return a 1D array
        assert len(sliced.shape) == 1
    else:
        assert len(sliced.shape) == 2
    if isinstance(sliced, pd.DataFrame) and isinstance(expected, pd.DataFrame):
        assert (sliced.index == expected.index).all()
        assert (sliced.columns == expected.columns).all()
    if isinstance(sliced, pd.Series) and isinstance(expected, pd.Series):
        assert (sliced.index == expected.index).all()
    assert np.array_equal(array_compat.to_np(sliced), array_compat.to_np(expected))
