import numpy as np
import pandas as pd
import pytest

pytest.importorskip("shap")
xgboost = pytest.importorskip("xgboost")

from thds.mllegos import shaplegos  # noqa: E402
from thds.mllegos.shaplegos import _xgb_compat  # noqa: E402

from .conftest import FEATURES, KEYS  # noqa: E402


def test_scalar_base_score_keeps_the_first_element_of_a_vector() -> None:
    assert _xgb_compat._scalar_base_score({"base_score": "[5E-1]"}) == {"base_score": "0.5"}
    # a multiclass model writes one element per class; shap 0.50 keeps the first, and so do we
    assert _xgb_compat._scalar_base_score({"base_score": "[0.1, 0.2]"}) == {"base_score": "0.1"}
    assert _xgb_compat._scalar_base_score({"base_score": "5E-1"}) == {"base_score": "5E-1"}
    assert _xgb_compat._scalar_base_score({}) == {}


def test_explain_tree_reproduces_multiclass_xgboost_margin(frame: pd.DataFrame) -> None:
    """A multiclass model's per-class `base_score` vector must not break the loader either."""
    X = shaplegos.to_float_frame(frame, FEATURES)
    classes = np.arange(len(frame)) % 3
    model = xgboost.XGBClassifier(
        objective="multi:softprob", n_estimators=10, max_depth=3, random_state=0
    ).fit(X, classes)
    keyed = shaplegos.explain_tree(
        model, frame, feature_columns=FEATURES, key_columns=KEYS, output_index=1
    )
    margin = model.predict(X, output_margin=True)[:, 1]
    reproduced = keyed.explanation.base_values + keyed.explanation.values.sum(axis=1)
    np.testing.assert_allclose(reproduced, margin, atol=1e-4)


def test_explain_tree_reproduces_xgboost_margin(frame: pd.DataFrame) -> None:
    """The point of the shim: shap 0.49 + xgboost >= 3.1 would raise before this assertion."""
    X = shaplegos.to_float_frame(frame, FEATURES)
    model = xgboost.XGBClassifier(n_estimators=10, max_depth=3, random_state=0).fit(X, frame["label"])
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    margin = model.predict(X, output_margin=True)
    reproduced = keyed.explanation.base_values + keyed.explanation.values.sum(axis=1)
    np.testing.assert_allclose(reproduced, margin, atol=1e-4)
