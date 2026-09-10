"""Binary-classification score computation on plain arrays and pandas frames.

Results are plain floats and ints, so callers can wrap them in whatever reporting types
their metrics systems require. Lives under `sklegos` for its eager sklearn import.
"""

import typing as ty
from functools import partial

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    fbeta_score,
    precision_score,
    recall_score,
)

from ..types import Any1DArray


class ConfusionCounts(ty.NamedTuple):
    """Binary confusion-matrix counts, for 0/1-encoded labels."""

    tp: int
    fp: int
    fn: int
    tn: int


class BinaryClsScores(ty.NamedTuple):
    """Standard binary classifier scores for a single evaluation task.

    `accuracy`/`precision`/`recall` are the usual point metrics; `f0_5`/`f1`/`f2` are F-beta
    scores weighting precision, balance, and recall respectively; `avg_precision` is the
    area under the precision-recall curve, computed from `y_score`.
    """

    accuracy: float
    precision: float
    recall: float
    f0_5: float
    f1: float
    f2: float
    avg_precision: float


def confusion_counts(y_true: ty.Sequence[int], y_pred: ty.Sequence[int]) -> ConfusionCounts:
    """Compute binary confusion counts; labels are fixed to [0, 1]."""
    tn, fp, fn, tp = (int(x) for x in confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel())
    return ConfusionCounts(tp=tp, fp=fp, fn=fn, tn=tn)


def binary_cls_scores(
    y_true: ty.Sequence[int],
    y_pred: ty.Sequence[int],
    y_score: ty.Sequence[float],
    *,
    zero_division: float = 0.0,
) -> BinaryClsScores:
    """Compute the standard binary classifier scores for 0/1-encoded labels.

    `zero_division` is returned for precision/recall/F-beta when their denominator is zero
    (e.g. no predicted positives).
    """
    return BinaryClsScores(
        accuracy=float(accuracy_score(y_true, y_pred)),
        precision=float(precision_score(y_true, y_pred, zero_division=zero_division)),
        recall=float(recall_score(y_true, y_pred, zero_division=zero_division)),
        f0_5=float(fbeta_score(y_true, y_pred, beta=0.5, zero_division=zero_division)),
        f1=float(fbeta_score(y_true, y_pred, beta=1.0, zero_division=zero_division)),
        f2=float(fbeta_score(y_true, y_pred, beta=2.0, zero_division=zero_division)),
        avg_precision=float(average_precision_score(y_true, y_score)),
    )


def cls_scores(
    y_true: Any1DArray,
    y_pred: Any1DArray,
    y_score: Any1DArray,
    pos_label: ty.Hashable,
    *,
    zero_division: float = 0.0,
) -> pd.Series:
    """Score labels/predictions of any dtype; `pos_label` names the positive class.

    Unlike `binary_cls_scores`, labels need not be 0/1-encoded. Returns a Series of
    accuracy/precision/recall/average_precision plus the row count under `samples`.
    """
    return pd.Series(
        dict(
            accuracy=accuracy_score(y_true, y_pred),
            precision=precision_score(y_true, y_pred, pos_label=pos_label, zero_division=zero_division),
            recall=recall_score(y_true, y_pred, pos_label=pos_label, zero_division=zero_division),
            average_precision=average_precision_score(y_true, y_score, pos_label=pos_label),
            samples=len(y_true),
        )
    )


def frame_cls_scores(
    df: pd.DataFrame,
    true_col: ty.Hashable,
    pred_col: ty.Hashable,
    pred_prob_col: ty.Hashable,
    pos_label: ty.Hashable,
    *,
    zero_division: float = 0.0,
) -> pd.Series:
    """`cls_scores` over the named columns of `df`, for callers with a frame in hand."""
    return cls_scores(
        df[true_col], df[pred_col], df[pred_prob_col], pos_label, zero_division=zero_division
    )


def score_by_variable(
    df: pd.DataFrame,
    variable: str,
    true_col: ty.Hashable,
    pred_col: ty.Hashable,
    pred_prob_col: ty.Hashable,
    pos_label: ty.Hashable,
    *,
    zero_division: float = 0.0,
) -> pd.DataFrame:
    """Apply `frame_cls_scores` per group of `variable`: one row of scores per group."""
    return df.groupby(variable).apply(
        partial(
            frame_cls_scores,
            true_col=true_col,
            pred_col=pred_col,
            pred_prob_col=pred_prob_col,
            pos_label=pos_label,
            zero_division=zero_division,
        )
    )  # type: ignore[return-value]
