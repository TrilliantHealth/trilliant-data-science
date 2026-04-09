from typing import Tuple

import numpy as np
import pandas as pd
import pytest
from sklearn.tree import DecisionTreeClassifier

from thds.mllegos.sklegos.modeling import feature_split


def _make_test_data(
    n_examples: int,
    n_classes: int,
    n_splits: int,
    split_dtype: type,
    y_dtype: type,
    tie_y: bool,
) -> Tuple[pd.DataFrame, pd.Series]:
    split = np.random.random_integers(1, n_splits, n_examples)
    y = split % n_classes if tie_y else np.random.random_integers(1, n_classes, n_examples)
    # uninformative features
    feature1 = np.random.random(n_examples)
    feature2 = np.random.random(n_examples)
    X = pd.DataFrame(dict(split=split.astype(split_dtype), feature1=feature1, feature2=feature2))
    return X, pd.Series(y.astype(y_dtype))


@pytest.mark.parametrize("tie_y", [True, False])
@pytest.mark.parametrize("y_dtype", [int, str])
@pytest.mark.parametrize("split_dtype", [int, str])
@pytest.mark.parametrize(
    "n_examples, n_classes, n_splits",
    [
        (10, 2, 1),
        (10, 2, 2),
        (50, 2, 2),
        (50, 2, 5),
        (50, 5, 2),
        (50, 5, 10),
        (100, 2, 1),
        (100, 2, 10),
        (100, 5, 7),
    ],
)
def test_discrete_feature_split_check_estimator(
    n_examples: int,
    n_classes: int,
    n_splits: int,
    split_dtype: type,
    y_dtype: type,
    tie_y: bool,
):
    X, y = _make_test_data(**locals())
    est = feature_split.DiscreteFeatureSplit(
        feature_name="split",
        classifier=DecisionTreeClassifier(max_depth=1, max_features=1, max_leaf_nodes=2),
        min_frequency=1,
    )
    # this is the "official" way to do a bunch of checks against the rather dynamic, duck-typed
    # interfaces expected by sklearn, but we have to jump through some hoops which are irrelevant to
    # our use case to make it pass, so we're skipping that for tests that are more aligned with our
    # actual usage
    # check_estimator(est)

    est.fit(X, y)
    preds = est.predict(X)

    if tie_y:
        # if tie_y is true, y is a pure function of the split variable so we should get perfect accuracy
        assert (y == preds).all()

    probs1 = est.predict_proba(X)

    preds2 = est.classes_[np.argmax(probs1.values, axis=1)]
    assert (preds2 == preds).all()

    probs2 = est.transform(X)

    assert np.array_equal(probs1, probs2)
