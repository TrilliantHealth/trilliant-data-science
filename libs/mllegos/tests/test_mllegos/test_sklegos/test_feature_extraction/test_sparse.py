import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from thds.mllegos.sklegos.feature_extraction.sparse import (
    DenseToSparseNamedTransformer,
    SparseCountFeatureNormalized,
    SparseCountFeatures,
    to_dense,
    to_sparse,
)

# to_sparse / to_dense


def test_to_sparse_wraps_dense_and_passes_csr_through() -> None:
    dense = np.array([[1, 0], [0, 2]])
    as_sparse = to_sparse(dense)
    assert isinstance(as_sparse, sparse.csr_matrix)
    assert as_sparse is to_sparse(as_sparse)


def test_to_sparse_converts_non_csr_sparse_to_csr() -> None:
    coo = sparse.coo_matrix(np.eye(2))
    assert isinstance(to_sparse(coo), sparse.csr_matrix)


def test_to_dense_round_trips() -> None:
    dense = np.array([[1.0, 0.0], [0.0, 2.0]])
    np.testing.assert_array_equal(to_dense(to_sparse(dense)), dense)


def test_to_dense_passes_dense_through_and_rejects_others() -> None:
    arr = np.array([1, 2])
    assert to_dense(arr) is arr
    series = pd.Series([1, 2])
    assert to_dense(series) is series
    with pytest.raises(TypeError, match="Unsupported type"):
        to_dense([1, 2])  # type: ignore[arg-type]


# DenseToSparseNamedTransformer


def test_dense_to_sparse_named_transformer_frame() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    transformer = DenseToSparseNamedTransformer().fit(df)
    assert transformer.get_feature_names_out() == ["a", "b"]
    result = transformer.transform(df)
    assert isinstance(result, sparse.csr_matrix)
    np.testing.assert_array_equal(result.toarray(), df.to_numpy())


def test_dense_to_sparse_named_transformer_series_uses_name() -> None:
    series = pd.Series([1, 2], name="counts")
    assert DenseToSparseNamedTransformer().fit(series).get_feature_names_out() == ["counts"]


# SparseCountFeatures

_STRUCT_ROWS = pd.Series(
    [
        [{"value": "A", "count": 3}, {"value": "B", "count": 5}],
        [{"value": "B", "count": 1}],
        None,
    ]
)


def test_sparse_count_features_infers_vocab() -> None:
    transformer = SparseCountFeatures(value_field="value", count_field="count", custom_vocab=None)
    matrix = transformer.fit(_STRUCT_ROWS).transform(_STRUCT_ROWS)
    assert list(transformer.get_feature_names_out()) == ["A", "B"]
    np.testing.assert_array_equal(matrix.toarray(), [[3, 5], [0, 1], [0, 0]])


def test_sparse_count_features_custom_vocab_restricts_terms() -> None:
    transformer = SparseCountFeatures(value_field="value", count_field="count", custom_vocab=["B"])
    matrix = transformer.fit(_STRUCT_ROWS).transform(_STRUCT_ROWS)
    assert list(transformer.get_feature_names_out()) == ["B"]
    np.testing.assert_array_equal(matrix.toarray(), [[5], [1], [0]])


def test_sparse_count_features_unfitted_raises() -> None:
    transformer = SparseCountFeatures(value_field="value", count_field="count", custom_vocab=None)
    with pytest.raises(ValueError, match="not been fitted"):
        transformer.get_feature_names_out()


# SparseCountFeatureNormalized


def test_sparse_count_feature_normalized_divides_by_dense_column() -> None:
    frame = pd.DataFrame({"structs": _STRUCT_ROWS, "total": [2.0, 4.0, 1.0]})
    inner = SparseCountFeatures(value_field="value", count_field="count", custom_vocab=None)
    transformer = SparseCountFeatureNormalized(
        normalize_by="total", sparse_feature="structs", sparse_feat_extractor=inner
    )
    result = transformer.fit(frame).transform(frame)
    np.testing.assert_array_equal(to_dense(result), [[1.5, 2.5], [0.0, 0.25], [0.0, 0.0]])
    assert list(transformer.get_feature_names_out()) == ["A", "B"]
