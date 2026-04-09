import typing as ty
from functools import singledispatch

import numpy as np
import pandas as pd

try:
    from scipy import sparse as sp
except ImportError:
    sp = None

from . import types

AnyIndexer = ty.Union[slice, types.Any1DArray, ty.List[int]]
_MaybeIndexer = ty.Optional[AnyIndexer]


@singledispatch
def to_np(X: types.Any2DArray) -> np.ndarray:
    raise TypeError(f"Don't know how to convert array of type {type(X)} to numpy array")


@to_np.register(np.ndarray)
def to_np_np(X: np.ndarray) -> np.ndarray:
    return X


@to_np.register(pd.Series)
@to_np.register(pd.DataFrame)
def to_np_pd(X: ty.Union[pd.Series, pd.DataFrame]) -> np.ndarray:
    return X.values  # type: ignore


if sp is not None:

    @to_np.register(sp.csr_matrix)
    @to_np.register(sp.csc_matrix)
    @to_np.register(sp.coo_matrix)
    def to_np_sp(X: types.AnySparseArray) -> np.ndarray:
        return X.toarray()


A = ty.TypeVar("A", bound=types.Any2DArray)


def _normalize_indexer(indexer: _MaybeIndexer) -> AnyIndexer:
    if indexer is None:
        return slice(None)
    return indexer


def _is_array_like(indexer: _MaybeIndexer) -> bool:
    return isinstance(indexer, (np.ndarray, pd.Series, list))


@singledispatch
def slice_2d(X: A, rows: _MaybeIndexer, cols: _MaybeIndexer) -> A:
    raise TypeError(f"Don't know how to slice array of type {type(X)}")


@slice_2d.register(np.ndarray)
def slice_2d_np(X: np.ndarray, rows: _MaybeIndexer, cols: _MaybeIndexer) -> np.ndarray:  # type: ignore[misc]
    if rows is None and cols is None:
        # minor optimization
        return X
    if _is_array_like(rows) and _is_array_like(cols):
        # numpy turns this into a 1d array; have to do in 2 steps
        return X[_normalize_indexer(rows)][:, _normalize_indexer(cols)]
    return X[_normalize_indexer(rows), _normalize_indexer(cols)]


@slice_2d.register(pd.DataFrame)
def slice_2d_pd(X: pd.DataFrame, rows: AnyIndexer, cols: AnyIndexer) -> pd.DataFrame:  # type: ignore[misc]
    if rows is None and cols is None:
        # minor optimization
        return X
    return X.iloc[_normalize_indexer(rows), _normalize_indexer(cols)]


if sp is not None:

    @slice_2d.register(sp.csr_matrix)
    @slice_2d.register(sp.csc_matrix)
    @slice_2d.register(sp.coo_matrix)
    def slice_2d_sp(X: types.AnySparseArray, rows: AnyIndexer, cols: AnyIndexer) -> sp.spmatrix:  # type: ignore[misc]
        if rows is None and cols is None:
            # minor optimization
            return X
        if _is_array_like(rows) and _is_array_like(cols):
            # numpy turns this into a 1d array; have to do in 2 steps
            return X[_normalize_indexer(rows)][:, _normalize_indexer(cols)]
        return X[_normalize_indexer(rows), _normalize_indexer(cols)]


@singledispatch
def hconcat(a: types.Any2DArray, b: types.Any2DArray) -> types.Any2DArray:
    raise TypeError(
        f"Don't know how to horizontally concatenate arrays of types {type(a)} and {type(b)}"
    )


@hconcat.register(np.ndarray)
def hconcat_np(*arrs: np.ndarray) -> np.ndarray:
    return np.hstack(arrs)


@hconcat.register(pd.DataFrame)
def hconcat_pd(*arrs: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(arrs, axis=1)


if sp is not None:

    @hconcat.register(sp.csr_matrix)
    @hconcat.register(sp.csc_matrix)
    @hconcat.register(sp.coo_matrix)
    def hconcat_sp(*arrs: types.AnySparseArray) -> types.AnySparseArray:
        return sp.hstack(arrs)
