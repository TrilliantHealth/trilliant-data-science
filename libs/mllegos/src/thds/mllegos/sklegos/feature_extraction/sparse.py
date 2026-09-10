"""Sparse-matrix conversion helpers and transformers for count-valued struct features.

`to_sparse` / `to_dense` are plain functions, safe to wrap in a
`sklearn.preprocessing.FunctionTransformer` inside a pickled Pipeline. The transformer classes
turn columns of `{value, count}` structs into sparse count matrices with stable feature names,
suitable for composition inside a `ColumnTransformer`.
"""

import operator
import typing as ty

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.base import BaseEstimator, TransformerMixin

AnyArray: ty.TypeAlias = pd.Series | np.ndarray | sparse.spmatrix


def to_sparse(X: AnyArray) -> sparse.csr_matrix:
    """Return `X` as a CSR matrix; dense input is wrapped without copying."""
    return X if isinstance(X, sparse.csr_matrix) else sparse.csr_matrix(X, copy=False)


def to_dense(X: AnyArray | pd.DataFrame) -> np.ndarray | pd.Series | pd.DataFrame:
    """Return a dense equivalent of `X`; dense input passes through unchanged."""
    if isinstance(X, sparse.spmatrix):
        return X.toarray()  # type: ignore[attr-defined]
    if isinstance(X, (np.ndarray, pd.Series, pd.DataFrame)):
        return X
    raise TypeError(f"Unsupported type passed to to_dense: {type(X)}")


class DenseToSparseNamedTransformer(BaseEstimator, TransformerMixin):
    """Convert dense arrays to sparse format while providing feature names.

    On fit, pulls feature names from `X` (the Series name, or DataFrame columns).
    """

    def __init__(self):
        self.feature_names_ = None

    def fit(self, X: pd.Series | pd.DataFrame, y=None):
        self.feature_names_ = [X.name] if isinstance(X, pd.Series) else X.columns.to_list()
        return self

    def transform(self, X):
        return to_sparse(X)

    def get_feature_names_out(self, input_features=None):
        # input_features is required by the ColumnTransformer interface
        if hasattr(self, "feature_names_"):
            return self.feature_names_
        raise ValueError("Transformer has not been fitted.")


class SparseCountFeatures(BaseEstimator, TransformerMixin):
    """Convert arrays of dicts (with a value and a count field) into a sparse count matrix.

    Columns correspond to unique value terms and entries are their counts. Optionally restricts
    the vocabulary to a predefined set of allowed terms (`custom_vocab`).

    Assumes each element of X is a list-like structure of dictionaries with keys:
    - `value_field` (str): the category/key name
    - `count_field` (str): the count associated with that category

    Example input (for each row in X):
    `[{'value_field': 'A', 'count_field': 3}, {'value_field': 'B', 'count_field': 5}]`
    """

    def __init__(self, value_field: str, count_field: str, custom_vocab: list[str] | None):
        self.value_field = value_field
        self.count_field = count_field
        self.custom_vocab = custom_vocab

    def fit(self, X: pd.Series, y=None):
        if self.custom_vocab is not None:
            # Use restricted terms as the vocabulary
            vocab = sorted(set(map(str, self.custom_vocab)))
        else:
            # Infer vocabulary from the data
            values = (
                X.explode().dropna().apply(operator.itemgetter(self.value_field)).unique().astype(str)
            )
            vocab = sorted(values)

        self.vocab_ = {term: i for i, term in enumerate(vocab)}
        return self

    def transform(self, X: AnyArray) -> sparse.csr_matrix:
        X = pd.Series(X, copy=False)
        X.index = pd.RangeIndex(len(X))
        structs = X.explode().dropna()
        counts = structs.apply(operator.itemgetter(self.count_field))
        indices = structs.apply(operator.itemgetter(self.value_field)).astype(str).map(self.vocab_)
        in_vocab = indices.notna().to_numpy(dtype=bool)
        # coerce to numeric: counts may arrive as strings or mixed types from struct columns
        counts = pd.to_numeric(counts[in_vocab], errors="coerce").fillna(0).astype(int)
        indices = indices[in_vocab].astype(int)

        return sparse.csr_matrix(
            (counts.values, (indices.index, indices.values)), shape=(len(X), len(self.vocab_))
        )

    def get_feature_names_out(self, input_features=None):
        if hasattr(self, "vocab_"):
            return [kv[0] for kv in sorted(self.vocab_.items(), key=lambda kv: kv[1])]
        raise ValueError("Transformer has not been fitted.")


class SparseCountFeatureNormalized(BaseEstimator, TransformerMixin):
    """Wrap a `SparseCountFeatures`, dividing each row's counts by a dense column of `X`."""

    def __init__(
        self, *, normalize_by: str, sparse_feature: str, sparse_feat_extractor: SparseCountFeatures
    ):
        self.normalize_by = normalize_by
        self.sparse_feature = sparse_feature
        self.sparse_feat_extractor = sparse_feat_extractor

    def fit(self, X: pd.DataFrame, y=None):
        self.sparse_feat_extractor.fit(X[self.sparse_feature], y)
        return self

    def transform(self, X: pd.DataFrame):
        sp_matrix = self.sparse_feat_extractor.transform(X[self.sparse_feature])
        counts = X[self.normalize_by].to_numpy(copy=False)
        return sp_matrix / counts[:, np.newaxis]

    def get_feature_names_out(self, input_features=None):
        # input_features is required by the ColumnTransformer interface
        return self.sparse_feat_extractor.get_feature_names_out()
