import pytest

pytest.importorskip("pyecharts")

from thds.mllegos.eval.viz.cls import confusion_matrix_heatmap  # noqa: E402


def test_confusion_matrix_heatmap_builds():
    chart = confusion_matrix_heatmap({"tp": 10, "fp": 2, "fn": 3, "tn": 85}, unit_label="claims")
    rendered = chart.dump_options()
    assert "claims" in rendered
    assert "Predicted" in rendered
    assert "Actual" in rendered


def test_confusion_matrix_heatmap_custom_colors():
    chart = confusion_matrix_heatmap(
        {"tp": 1, "fp": 0, "fn": 0, "tn": 0},
        range_color=("#000000", "#ffffff"),
    )
    assert "#ffffff" in chart.dump_options()
