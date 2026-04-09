import typing as ty

import numpy as np
import pandas as pd
import pytest
from scipy.sparse import csr_matrix
from sklearn.base import clone
from sklearn.utils.validation import check_is_fitted

from thds.mllegos.sklegos import array_compat, types
from thds.mllegos.sklegos.feature_selection import dynamic, univariate

ArrayType = ty.Literal["np", "pd", "sparse"]


def make_dataset(
    array_type: ArrayType,
    n: int,
    n_classes: int,
    n_const_features: int = 0,
    n_correlated_features: int = 0,
    n_random_features: int = 0,
) -> tuple[types.Any2DArray, np.ndarray]:
    """Make X and y arrays for testing. X is a horizontal concat of random, then correlated, then constant features.
    y is a vertical repeat of the class labels. The random features are positive, the correlated features are shifted
    copies of the class label (by a different offset for each feature), and the constant features are all zeros.
    """
    assert n % n_classes == 0
    y = np.repeat(np.arange(n_classes), n // n_classes)
    if n_correlated_features:
        corr_features = np.concatenate(
            [(y + i).reshape(n, 1) for i in range(n_correlated_features)], axis=1
        )
    else:
        corr_features = np.empty((n, 0))
    zeros = np.zeros((n, n_const_features))
    np.random.seed(1729)
    random = np.abs(np.random.randn(n, n_random_features))
    X = np.concatenate([random, corr_features, zeros], axis=1)
    if array_type == "pd":
        X_ = pd.DataFrame(X)
    elif array_type == "sparse":
        X_ = csr_matrix(X)
    else:
        X_ = X
    return X_, y


def assert_fitted_attrs_nonnull(
    fs: dynamic.DynamicFeatureSelection,
    low_var: bool = False,
    main: bool = False,
    final: bool = False,
    aux: bool = False,
):
    for flag, selector in (
        (low_var, fs.non_constant_selector_),
        (main, fs.feature_selector_),
        (final, fs.final_feature_selector_),
        (aux, fs.aux_feature_selector_),
    ):
        if flag:
            assert selector is not None
            check_is_fitted(selector)
        else:
            assert selector is None


def assert_not_fitted_attrs(fs: dynamic.DynamicFeatureSelection):
    for attr in (
        "non_constant_selector_",
        "feature_selector_",
        "final_feature_selector_",
        "aux_feature_selector_",
    ):
        assert not hasattr(fs, attr)


def test_clone():
    X, y = make_dataset("np", 100, 2, n_const_features=2, n_correlated_features=2, n_random_features=2)
    fs = dynamic.DynamicFeatureSelection(
        feature_selector=univariate.construct_univariate_fs("FDR", "chi2", 0.05),
        feature_score="MI",
        min_features=1,
        max_features=None,
    )
    fs_clone = clone(fs)
    fs.fit(X, y)
    check_is_fitted(fs)
    with pytest.raises(ValueError):
        check_is_fitted(fs_clone)
    assert_not_fitted_attrs(fs_clone)


def slice_2d(X, row, col) -> np.ndarray:
    return array_compat.to_np(array_compat.slice_2d(X, row, col))


@pytest.mark.parametrize("array_type", list(ty.get_args(ArrayType)))
def test_dynamic_fs_low_variance(array_type: ArrayType):
    n = 100
    X, y = make_dataset(array_type, n, 2, n_const_features=10, n_random_features=1)
    # first column is the random feature
    the_feature = array_compat.slice_2d(X, None, 0)  # type: ignore[arg-type]
    fs = dynamic.DynamicFeatureSelection(
        feature_selector=univariate.construct_univariate_fs("FDR", "chi2", 0.05),
        feature_score="MI",
        min_features=1,
        max_features=None,
    )
    fs.fit(X, y)
    assert_fitted_attrs_nonnull(fs, low_var=True)
    assert fs.get_support(indices=True).tolist() == [0]
    assert fs.get_feature_names_out().tolist() == ["x0"]
    X_transformed = fs.transform(X)
    assert X_transformed.shape == (n, 1)
    assert np.array_equal(slice_2d(X_transformed, None, 0), array_compat.to_np(the_feature))


@pytest.mark.parametrize("array_type", list(ty.get_args(ArrayType)))
def test_dynamic_fs_with_aux_feature_selection(array_type: ArrayType):
    n = 500
    n_corr = 2
    n_rand = 4
    min_features = 4
    corr_ixs_selected = [4, 5]
    X, y = make_dataset(
        array_type, n, 2, n_const_features=5, n_correlated_features=n_corr, n_random_features=n_rand
    )
    fs = dynamic.DynamicFeatureSelection(
        feature_selector=univariate.construct_univariate_fs("FDR", "F", 0.01),
        feature_score="F",
        min_features=min_features,
        # only 2 correlated features; we have to pick some more random ones to get to min_features
        max_features=None,
    )
    fs.fit(X, y)
    assert_fitted_attrs_nonnull(fs, low_var=True, main=True, aux=True)
    ixs_selected = fs.get_support(indices=True).tolist()
    assert ixs_selected[-len(corr_ixs_selected) :] == corr_ixs_selected
    assert all(i < n_rand for i in ixs_selected[: -len(corr_ixs_selected)])
    # assert that the auxiliary features selected were the random features and not the constant ones
    assert fs.get_feature_names_out().tolist() == [f"x{i}" for i in ixs_selected]

    X_transformed = fs.transform(X)
    assert X_transformed.shape == (n, min_features)
    assert np.allclose(
        array_compat.to_np(X_transformed),
        slice_2d(X, None, ixs_selected),
    )


@pytest.mark.parametrize("array_type", list(ty.get_args(ArrayType)))
def test_dynamic_fs_without_aux_feature_selection(array_type: ArrayType):
    n = 500
    n_corr = 4
    n_rand = 5
    min_features = 3
    max_features = 4
    ixs_selected = [5, 6, 7, 8]
    # enough correlated features that no auxiliary feature selection is needed
    # but *not* enough that we will have to perform an extra round of selection to get below max_features
    X, y = make_dataset(
        array_type, n, 2, n_const_features=0, n_correlated_features=n_corr, n_random_features=n_rand
    )
    fs = dynamic.DynamicFeatureSelection(
        feature_selector=univariate.construct_univariate_fs("FDR", "chi2", 0.01),
        # this selects n_corr, which is too many
        feature_score="chi2",
        min_features=min_features,
        max_features=max_features,
    )
    fs.fit(X, y)
    assert_fitted_attrs_nonnull(fs, low_var=False, main=True, final=False)
    assert fs.get_support(indices=True).tolist() == ixs_selected
    assert fs.get_feature_names_out().tolist() == [f"x{i}" for i in ixs_selected]
    # not constant features so no lov-variance selector
    X_transformed = fs.transform(X)
    assert X_transformed.shape == (n, max_features)
    X_transformed_ = array_compat.to_np(X_transformed)
    X_ = slice_2d(X, None, ixs_selected)
    assert np.allclose(X_transformed_, X_)


@pytest.mark.parametrize("array_type", list(ty.get_args(ArrayType)))
def test_dynamic_fs_with_final_feature_selection(array_type: ArrayType):
    n = 500
    n_corr = 6
    n_rand = 4
    min_features = 3
    max_features = 4
    ixs_selected = [4, 5, 6, 7]
    # enough correlated features that no auxiliary feature selection is needed
    # but we will have to perform an extra round of selection to get below max_features
    X, y = make_dataset(
        array_type, n, 2, n_const_features=5, n_correlated_features=n_corr, n_random_features=n_rand
    )
    fs = dynamic.DynamicFeatureSelection(
        feature_selector=univariate.construct_univariate_fs("FDR", "chi2", 0.01),
        # this selects n_corr, which is too many
        feature_score="chi2",
        min_features=min_features,
        max_features=max_features,
    )
    fs.fit(X, y)
    assert_fitted_attrs_nonnull(fs, low_var=True, main=True, final=True)
    assert fs.get_support(indices=True).tolist() == ixs_selected
    assert fs.get_feature_names_out().tolist() == [f"x{i}" for i in ixs_selected]
    X_transformed = fs.transform(X)
    assert X_transformed.shape == (n, max_features)
    X_transformed_ = array_compat.to_np(X_transformed)
    X_ = slice_2d(X, None, ixs_selected)
    assert np.allclose(X_transformed_, X_)
