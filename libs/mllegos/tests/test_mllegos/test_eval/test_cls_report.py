import numpy as np
import pandas as pd

from thds.mllegos.eval import cls_report


def _build_report_dict() -> dict:
    return {
        "class_a": {
            "precision": np.float64(0.8),
            "recall": np.float64(0.75),
            "f1-score": np.float64(0.77),
            "support": np.int64(20),
        },
        "class_b": {
            "precision": np.float64(0.6),
            "recall": np.float64(0.65),
            "f1-score": np.float64(0.62),
            "support": np.int64(15),
        },
        "accuracy": np.float64(0.72),
        "macro avg": {
            "precision": np.float64(0.7),
            "recall": np.float64(0.7),
            "f1-score": np.float64(0.7),
            "support": np.int64(35),
        },
        "weighted avg": {
            "precision": np.float64(0.72),
            "recall": np.float64(0.72),
            "f1-score": np.float64(0.72),
            "support": np.int64(35),
        },
    }


def test_multiclass_performance_viz_handles_numpy_scalars() -> None:
    report_dict = _build_report_dict()
    context = pd.DataFrame(
        {
            "classification": ["A", "B"],
            "description": ["Alpha", "Beta"],
        },
        index=["class_a", "class_b"],
    )

    chart = cls_report.multiclass_performance_viz(
        report_dict,
        x_field="precision",
        y_field="recall",
        context_info=context,
        color_by="classification",
    )

    # If numpy scalar values leak through to pyecharts the JSON dump raises a TypeError.
    chart_options = chart.dump_options()

    assert "class_a" in chart_options
    assert "class_b" in chart_options
