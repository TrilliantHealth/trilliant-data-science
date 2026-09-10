import numpy as np
import pandas as pd
import pytest

from thds.mllegos.sklegos.eval import binary_cls

Y_TRUE = [1, 1, 0, 0, 1]
Y_PRED = [1, 0, 0, 1, 1]
Y_SCORE = [0.9, 0.4, 0.2, 0.6, 0.8]


def test_confusion_counts() -> None:
    assert binary_cls.confusion_counts(Y_TRUE, Y_PRED) == binary_cls.ConfusionCounts(
        tp=2, fp=1, fn=1, tn=1
    )


def test_confusion_counts_single_class_input_keeps_binary_shape() -> None:
    assert binary_cls.confusion_counts([1, 1], [1, 1]) == binary_cls.ConfusionCounts(
        tp=2, fp=0, fn=0, tn=0
    )


def test_binary_cls_scores() -> None:
    scores = binary_cls.binary_cls_scores(Y_TRUE, Y_PRED, Y_SCORE)
    assert scores.accuracy == pytest.approx(3 / 5)
    assert scores.precision == pytest.approx(2 / 3)
    assert scores.recall == pytest.approx(2 / 3)
    assert scores.f1 == pytest.approx(2 / 3)
    # precision at each positive hit, descending by score: 1/1, 2/2, 3/4 -> mean 11/12
    assert scores.avg_precision == pytest.approx(11 / 12)


def test_binary_cls_scores_zero_division() -> None:
    no_predicted_positives = binary_cls.binary_cls_scores([1, 0], [0, 0], [0.1, 0.2], zero_division=1.0)
    assert no_predicted_positives.precision == 1.0
    assert binary_cls.binary_cls_scores([1, 0], [0, 0], [0.1, 0.2]).precision == 0.0


_FRAME = pd.DataFrame(
    {
        "actual": ["a", "a", "b", "b"],
        "predicted": ["a", "b", "b", "b"],
        "proba": [0.9, 0.4, 0.1, 0.2],
        "group": ["g1", "g1", "g1", "g2"],
    }
)


def test_cls_scores_accepts_plain_arrays() -> None:
    scores = binary_cls.cls_scores(
        np.array(["a", "a", "b", "b"]),
        np.array(["a", "b", "b", "b"]),
        np.array([0.9, 0.4, 0.1, 0.2]),
        pos_label="a",
    )
    assert scores["accuracy"] == pytest.approx(3 / 4)
    assert scores["precision"] == pytest.approx(1.0)
    assert scores["recall"] == pytest.approx(1 / 2)
    assert scores["samples"] == 4


def test_frame_cls_scores_with_string_labels() -> None:
    scores = binary_cls.frame_cls_scores(_FRAME, "actual", "predicted", "proba", pos_label="a")
    assert scores["accuracy"] == pytest.approx(3 / 4)
    assert scores["precision"] == pytest.approx(1.0)
    assert scores["recall"] == pytest.approx(1 / 2)
    assert scores["samples"] == 4


def test_score_by_variable_scores_each_group() -> None:
    by_group = binary_cls.score_by_variable(
        _FRAME, "group", "actual", "predicted", "proba", pos_label="a", zero_division=1.0
    )
    assert list(by_group.index) == ["g1", "g2"]
    assert by_group.loc["g1", "samples"] == 3
    assert by_group.loc["g2", "precision"] == 1.0  # no predicted positives -> zero_division
