"""Build `shap.Explanation`s from named feature frames, with rows addressable by key.

`shap` itself is indexed positionally. `KeyedExplanation` carries the caller's key columns
alongside the explanation so that lookup is one call. `explain_tree` does the frame -> float matrix
-> explanation plumbing.
"""

import dataclasses
import typing as ty

import numpy as np
import numpy.typing as npt
import pandas as pd
import shap

from ._xgb_compat import ensure_xgboost_base_score_compat

ModelOutput = ty.Literal["raw", "probability"]


def to_float_frame(
    frame: pd.DataFrame, columns: ty.Sequence[str], dtype: npt.DTypeLike = np.float32
) -> pd.DataFrame:
    """Select `columns` from `frame` as a float frame: bools become 0/1, missing values NaN.

    This is the numeric view tree libraries score on (NaN is a first-class "missing" to XGBoost and
    LightGBM), kept as a DataFrame so `shap` picks up the column names. `dtype` defaults to float32,
    the width most tree libraries compute in; pass float64 if the model was fit on float64 and the
    additivity check objects to the rounding.
    """
    sub = frame[list(columns)]
    values = sub.to_numpy(dtype=dtype, na_value=np.nan)
    return pd.DataFrame(values, columns=list(columns), index=sub.index)


@dataclasses.dataclass(frozen=True)
class KeyedExplanation:
    """A `shap.Explanation` whose rows can be looked up by key.

    `explanation.values` is 2-D (instances x features). `keys` is a `pd.MultiIndex` with one entry
    per instance, in the same order, and one named level per key column - the columns of the
    explained frame that identify a row (a customer id, a date, ...). Keys must be unique, so
    `locate` and `row` take the whole key as keyword arguments, one per level.
    `pd.MultiIndex.from_frame(frame[key_columns])` builds `keys` from a frame.
    """

    explanation: shap.Explanation
    keys: pd.MultiIndex

    def __post_init__(self) -> None:
        values = np.asarray(self.explanation.values)
        if values.ndim != 2:
            raise ValueError(
                f"expected 2-D SHAP values (instances x features); got shape {values.shape}. "
                "Pass `output_index` to `explain_tree` for a multi-output model."
            )
        if len(self.keys) != values.shape[0]:
            raise ValueError(f"{len(self.keys)} key rows for {values.shape[0]} explained instances")
        if not self.keys.is_unique:
            raise ValueError(f"keys must be unique; {self.keys.duplicated().sum()} rows repeat one")
        if any(name is None for name in self.keys.names):
            raise ValueError(f"every key level needs a name; have {list(self.keys.names)}")

    def __len__(self) -> int:
        return len(self.keys)

    @property
    def feature_names(self) -> tuple[str, ...]:
        return tuple(self.explanation.feature_names)

    @property
    def key_names(self) -> tuple[str, ...]:
        return tuple(self.keys.names)

    def locate(self, **key: object) -> int:
        """Return the position of the row whose key is `key` (one keyword per key level).

        Raise KeyError when a level is missing or unknown, or when no row has that key.
        """
        if set(key) != set(self.key_names):
            raise KeyError(
                f"key levels are {list(self.key_names)}; "
                f"missing {sorted(set(self.key_names) - set(key))}, "
                f"unknown {sorted(set(key) - set(self.key_names))}"
            )
        full_key = tuple(key[name] for name in self.key_names)
        try:
            position = self.keys.get_loc(full_key)
        except KeyError:
            raise KeyError(f"no row with key {dict(zip(self.key_names, full_key))}") from None
        # a unique index resolves a whole key to one position, never a slice or mask
        assert isinstance(position, (int, np.integer)), position
        return int(position)

    def row(self, **key: object) -> shap.Explanation:
        """The single-instance explanation for `key` (the input to a waterfall plot)."""
        return self.explanation[self.locate(**key)]

    def take(self, positions: ty.Sequence[int]) -> "KeyedExplanation":
        """A new `KeyedExplanation` over the rows at `positions`, in that order."""
        idx = list(positions)
        return KeyedExplanation(self.explanation[idx], self.keys.take(idx))


def _tree_explainer(
    model: ty.Any,
    background: pd.DataFrame | None,
    model_output: ModelOutput,
    columns: ty.Sequence[str],
    dtype: npt.DTypeLike,
) -> shap.TreeExplainer:
    if model_output == "probability" and background is None:
        raise ValueError("model_output='probability' needs `background` rows to marginalize over")
    ensure_xgboost_base_score_compat()
    if background is None:
        return shap.TreeExplainer(model, model_output=model_output, feature_names=list(columns))
    return shap.TreeExplainer(
        model,
        data=to_float_frame(background, columns, dtype),
        model_output=model_output,
        feature_perturbation="interventional",
        feature_names=list(columns),
    )


def _select_output(explanation: shap.Explanation, output_index: int | None) -> shap.Explanation:
    """Reduce a multi-output explanation (instances x features x outputs) to one output."""
    if np.asarray(explanation.values).ndim < 3:
        return explanation
    if output_index is None:
        raise ValueError(
            f"model has {np.asarray(explanation.values).shape[-1]} outputs; pass `output_index`"
        )
    return explanation[..., output_index]


def explain_tree(
    model: ty.Any,
    frame: pd.DataFrame,
    *,
    feature_columns: ty.Sequence[str] | None = None,
    key_columns: ty.Sequence[str],
    background: pd.DataFrame | None = None,
    model_output: ModelOutput = "raw",
    output_index: int | None = None,
    check_additivity: bool = True,
    dtype: npt.DTypeLike = np.float32,
) -> KeyedExplanation:
    """Explain `model`'s predictions on the rows of `frame` with `shap.TreeExplainer`.

    :param model: any fitted tree ensemble `TreeExplainer` supports (xgboost, lightgbm, sklearn
        forests and gradient boosting, ...).
    :param frame: the rows to explain; key columns and feature columns side by side.
    :param feature_columns: the model's inputs, in the order the model was fit on. Defaults to
        every column of `frame` that is not a key column - so a label or any other non-feature
        column left in `frame` would be sent to the model; drop those first or pass this
        explicitly. Values pass through `to_float_frame`.
    :param key_columns: the columns that identify a row of `frame` (at least one); together they
        must be unique across `frame`. They become the levels of the result's `keys`.
    :param background: rows to marginalize over. Optional for `"raw"` output (the explainer then
        uses the trees' own cover statistics); required for `"probability"`. A few hundred to a few
        thousand representative rows is typical.
    :param model_output: `"raw"` explains the model's native output - log-odds for a binary
        classifier - and a row's `base_value + values.sum()` reproduces it exactly.
        `"probability"` explains the predicted probability instead.
    :param output_index: which output to keep for a multi-output model (e.g. the class of a
        multiclass classifier, or class 1 of an sklearn forest, whose `predict_proba` explanations
        come per class). Ignored for single-output models; required otherwise.
    :param check_additivity: forwarded to `shap`; leave on unless it raises for a model whose
        outputs `shap` cannot reproduce exactly.
    :param dtype: the float width features are cast to (see `to_float_frame`).

    Cost is roughly linear in rows and in the total number of tree leaves, so explain a sample of a
    large scoring universe and reserve full-frame runs for the specific rows you need to diagnose.
    """
    key_columns = tuple(key_columns)
    if not key_columns:
        raise ValueError("key_columns must name at least one column")
    columns = (
        tuple(feature_columns)
        if feature_columns is not None
        else tuple(c for c in frame.columns if c not in key_columns)
    )
    overlap = set(columns) & set(key_columns)
    if overlap:
        raise ValueError(f"columns cannot be both key and feature: {sorted(overlap)}")
    explainer = _tree_explainer(model, background, model_output, columns, dtype)
    explanation = explainer(to_float_frame(frame, columns, dtype), check_additivity=check_additivity)
    return KeyedExplanation(
        _select_output(explanation, output_index),
        pd.MultiIndex.from_frame(frame[list(key_columns)]),
    )


def mean_abs_shap(keyed: KeyedExplanation) -> pd.Series:
    """Global feature importance: mean |SHAP value| per feature, largest first.

    This is the quantity the `bar` plot draws; the Series form is for tables, joins and diffs
    between models.
    """
    importance = np.abs(np.asarray(keyed.explanation.values)).mean(axis=0)
    return (
        pd.Series(importance, index=list(keyed.feature_names), name="mean_abs_shap")
        .sort_values(ascending=False)
        .rename_axis("feature")
    )


class RowContributions(ty.NamedTuple):
    """One instance's explanation in tabular form.

    `output == base_value + table["shap_value"].sum()` is the model output the explanation is in
    (raw or probability, per `explain_tree`). `table` has one row per feature - `feature`, `value`
    (the instance's feature value) and `shap_value` - ordered by |shap_value| descending.
    """

    base_value: float
    output: float
    table: pd.DataFrame


def row_contributions(keyed: KeyedExplanation, **key: object) -> RowContributions:
    """Tabulate the per-feature contributions for the instance identified by `key`."""
    row = keyed.row(**key)
    values = np.asarray(row.values, dtype=float)
    base_value = float(np.asarray(row.base_values).reshape(-1)[0])
    table = pd.DataFrame(
        {
            "feature": list(keyed.feature_names),
            "value": np.asarray(row.data),
            "shap_value": values,
        }
    )
    order = np.argsort(-np.abs(values), kind="stable")
    return RowContributions(
        base_value=base_value,
        output=base_value + float(values.sum()),
        table=table.iloc[order].reset_index(drop=True),
    )
