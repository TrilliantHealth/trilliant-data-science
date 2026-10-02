import numpy as np
import pandas as pd
import pytest

pytest.importorskip("shap")
pytest.importorskip("sklearn")

from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier  # noqa: E402

from thds.mllegos import shaplegos  # noqa: E402

from .conftest import FEATURES, KEYS, N_ROWS  # noqa: E402

# --- to_float_frame ---


def test_to_float_frame_casts_bools_and_keeps_names(frame: pd.DataFrame) -> None:
    out = shaplegos.to_float_frame(frame, FEATURES)
    assert list(out.columns) == list(FEATURES)
    assert (out.dtypes == np.float32).all()
    assert set(out["flag"].unique()) <= {0.0, 1.0}


def test_to_float_frame_turns_missing_into_nan() -> None:
    frame = pd.DataFrame(
        {"a": pd.array([1, None], dtype="Int64"), "b": pd.array([True, None], dtype="boolean")}
    )
    out = shaplegos.to_float_frame(frame, ["a", "b"])
    assert out.isna().to_numpy().tolist() == [[False, False], [True, True]]
    assert out.iloc[0].tolist() == [1.0, 1.0]


def test_to_float_frame_honors_dtype(frame: pd.DataFrame) -> None:
    out = shaplegos.to_float_frame(frame, FEATURES, dtype=np.float64)
    assert (out.dtypes == np.float64).all()
    np.testing.assert_array_equal(out["signal"], frame["signal"])


# --- explain_tree ---


def test_explain_tree_reproduces_raw_output(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    assert len(keyed) == N_ROWS
    assert keyed.feature_names == FEATURES
    assert keyed.key_names == KEYS
    assert keyed.keys.is_unique
    raw = model.decision_function(shaplegos.to_float_frame(frame, FEATURES).to_numpy())
    reproduced = keyed.explanation.base_values + keyed.explanation.values.sum(axis=1)
    np.testing.assert_allclose(reproduced, raw, atol=1e-4)


def test_explain_tree_defaults_features_to_non_key_columns(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame.drop(columns="label"), key_columns=KEYS)
    assert keyed.feature_names == FEATURES


def test_explain_tree_requires_a_key_column(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    with pytest.raises(ValueError, match="at least one"):
        shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=())


def test_explain_tree_rejects_key_feature_overlap(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    with pytest.raises(ValueError, match="both key and feature"):
        shaplegos.explain_tree(
            model, frame, feature_columns=FEATURES, key_columns=("customer_id", "signal")
        )


def test_explain_tree_probability_needs_background(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    with pytest.raises(ValueError, match="background"):
        shaplegos.explain_tree(
            model, frame, feature_columns=FEATURES, key_columns=KEYS, model_output="probability"
        )


def test_explain_tree_probability_reproduces_predict_proba(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    rows = frame.iloc[:20]
    keyed = shaplegos.explain_tree(
        model,
        rows,
        feature_columns=FEATURES,
        key_columns=KEYS,
        background=frame,
        model_output="probability",
    )
    proba = model.predict_proba(shaplegos.to_float_frame(rows, FEATURES).to_numpy())[:, 1]
    reproduced = keyed.explanation.base_values + keyed.explanation.values.sum(axis=1)
    np.testing.assert_allclose(reproduced, proba, atol=1e-3)


def test_explain_tree_multi_output_requires_output_index(frame: pd.DataFrame) -> None:
    forest = RandomForestClassifier(n_estimators=5, max_depth=3, random_state=0).fit(
        shaplegos.to_float_frame(frame, FEATURES), frame["label"]
    )
    with pytest.raises(ValueError, match="output_index"):
        shaplegos.explain_tree(forest, frame, feature_columns=FEATURES, key_columns=KEYS)
    keyed = shaplegos.explain_tree(
        forest, frame, feature_columns=FEATURES, key_columns=KEYS, output_index=1
    )
    proba = forest.predict_proba(shaplegos.to_float_frame(frame, FEATURES))[:, 1]
    reproduced = keyed.explanation.base_values + keyed.explanation.values.sum(axis=1)
    np.testing.assert_allclose(reproduced, proba, atol=1e-4)


# --- KeyedExplanation lookup ---


def test_locate_and_row_by_key(model: GradientBoostingClassifier, frame: pd.DataFrame) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    target = frame.iloc[17]
    position = keyed.locate(customer_id=target["customer_id"], region=target["region"])
    assert position == 17
    row = keyed.row(customer_id=target["customer_id"], region=target["region"])
    np.testing.assert_array_equal(row.values, keyed.explanation.values[17])
    np.testing.assert_array_equal(row.data, keyed.explanation.data[17])


def test_locate_rejects_absent_partial_and_unknown_keys(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    with pytest.raises(KeyError, match="no row with key"):
        keyed.locate(customer_id=-1, region="r0")
    with pytest.raises(KeyError, match="missing \\['region'\\]"):
        keyed.locate(customer_id=1000)
    with pytest.raises(KeyError, match="unknown \\['unknown'\\]"):
        keyed.locate(customer_id=1000, region="r0", unknown="x")


def test_locate_works_with_a_single_key_column(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=("customer_id",))
    assert keyed.locate(customer_id=frame["customer_id"][9]) == 9


def test_take_subsets_rows_in_order(model: GradientBoostingClassifier, frame: pd.DataFrame) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    subset = keyed.take([5, 2])
    assert len(subset) == 2
    assert list(subset.keys.get_level_values("customer_id")) == [
        frame["customer_id"][5],
        frame["customer_id"][2],
    ]
    np.testing.assert_array_equal(subset.explanation.values[1], keyed.explanation.values[2])
    assert subset.locate(customer_id=frame["customer_id"][2], region=frame["region"][2]) == 1


def test_keyed_explanation_validates_shapes(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    with pytest.raises(ValueError, match="key rows"):
        shaplegos.KeyedExplanation(keyed.explanation, keyed.keys[:3])
    with pytest.raises(ValueError, match="2-D"):
        shaplegos.KeyedExplanation(keyed.explanation[0], keyed.keys[:1])


def test_keyed_explanation_requires_unique_named_keys(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    with pytest.raises(ValueError, match="unique"):
        shaplegos.KeyedExplanation(keyed.explanation, pd.MultiIndex.from_frame(frame[["region"]]))
    with pytest.raises(ValueError, match="name"):
        shaplegos.KeyedExplanation(
            keyed.explanation, pd.MultiIndex.from_arrays([frame["customer_id"]], names=[None])
        )


# --- tables ---


def test_mean_abs_shap_is_sorted_and_finds_the_signal(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    importance = shaplegos.mean_abs_shap(keyed)
    assert importance.index.name == "feature"
    assert importance.name == "mean_abs_shap"
    assert importance.index[0] == "signal"
    assert importance.is_monotonic_decreasing
    np.testing.assert_allclose(importance["noise"], np.abs(keyed.explanation.values[:, 1]).mean())


def test_row_contributions_sums_to_output(
    model: GradientBoostingClassifier, frame: pd.DataFrame
) -> None:
    keyed = shaplegos.explain_tree(model, frame, feature_columns=FEATURES, key_columns=KEYS)
    target = frame.iloc[3]
    contrib = shaplegos.row_contributions(
        keyed, customer_id=target["customer_id"], region=target["region"]
    )
    assert list(contrib.table.columns) == ["feature", "value", "shap_value"]
    assert set(contrib.table["feature"]) == set(FEATURES)
    assert contrib.table["shap_value"].abs().is_monotonic_decreasing
    assert contrib.table.set_index("feature").loc["signal", "value"] == pytest.approx(
        target["signal"], rel=1e-5
    )
    raw = model.decision_function(shaplegos.to_float_frame(frame.iloc[[3]], FEATURES).to_numpy())[0]
    assert contrib.output == pytest.approx(raw, abs=1e-4)
    assert contrib.base_value + contrib.table["shap_value"].sum() == pytest.approx(contrib.output)
