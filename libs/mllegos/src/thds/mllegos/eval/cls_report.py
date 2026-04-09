"""
Generic utils for working with the output of sklearn.metrics.classification_report (when `output_dict=True`)
1 - turn sklearn dict into a pandas dataframe
3 - make a scatterplot of classification stats
"""

from __future__ import annotations

import typing as ty

import numpy as np
import pandas as pd

if ty.TYPE_CHECKING:
    from pyecharts.charts import Scatter

from thds.core.log import getLogger

LOGGER = getLogger(__name__)


PERFORMANCE_METRIC = ty.Literal["precision", "recall", "f1-score", "support"]
CLASS_COLUMN = "class"
COLOR_BY_COLUMN = "__color_by__"
COLOR_BY_TYPES = ["integer", "boolean", "string"]


def to_pandas(report_dict: dict, context_info: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Converts an sklearn classification report dict to a pandas DataFrame. Optionally joins
    context info.

    Inputs:
        - report_dict: output of sklearn.metrics.classification_report when `output_dict=True`
        - context_info: pandas dataframe containing extra class info; indexed by class labels

    Returns:
        - pandas DataFrame
    """
    _report_dict = report_dict.copy()
    _report_dict.update(
        {
            "accuracy": {
                "precision": np.nan,
                "recall": np.nan,
                "f1-score": report_dict["accuracy"],
                "support": report_dict["macro avg"]["support"],
            }
        }
    )
    report_df = pd.DataFrame(_report_dict).transpose()

    if context_info is not None:
        name_collisions = set(ty.get_args(PERFORMANCE_METRIC)).intersection(set(context_info.columns))
        assert name_collisions == set(), (
            f"Cannot use performance metric names in context_info: {name_collisions}"
        )
        report_df = report_df.join(context_info, how="left")

    return report_df


def multiclass_performance_viz(
    report_dict: dict,
    x_field: PERFORMANCE_METRIC,
    y_field: PERFORMANCE_METRIC,
    context_info: pd.DataFrame | None = None,
    color_by: str | None = None,
) -> Scatter:
    """
    Visualize multi-class sklearn report using a scatterplot.

    Makes an interactive pyecharts scatterplot of class-wise performance metrics specified
    by x_field and y_field. Optionally joins with context_info to show more data
    in the tooltip & colors class points using `color_by` field.

    Inputs:
        - report_dict: output of sklearn.metrics.classification_report when `output_dict=True`
        - x_field: x-axis metric (precision, recall, f1-score, support)
        - y_field: y-axis metric (precision, recall, f1-score, support)
        - context_info: pandas dataframe containing extra info to be shown in the tooltip, indexed by class labels
        - color_by: field for coloring scatterplot points; may be 'class' to color by class labels or any string/int/boolean field in context_info.
            note: fields with more than 10 unique values will result in repeated colors

    Returns:
        - pyecharts Scatter chart

    To render in a jupyter notebook, call `chart.render_notebook()`
    To save as an html, call `chart.render('path/to/chart.html')`
    """
    from .viz import basic

    report_df = to_pandas(report_dict, context_info)

    class_stats = report_df.drop(["accuracy", "macro avg", "weighted avg"]).reset_index(
        names=CLASS_COLUMN
    )
    class_stats[["precision", "recall", "f1-score"]] = class_stats[
        ["precision", "recall", "f1-score"]
    ].round(4)

    null_stats = class_stats[[x_field, y_field]].isna().any(axis=1)
    if null_stats.any():
        if null_stats.all():
            LOGGER.error(f"All classes have null {x_field} and/or {y_field} values; cannot create plot")

        if null_stats.sum() > 7:
            null_class_list = ", ".join(class_stats.loc[null_stats, CLASS_COLUMN].tolist()[:7]) + " ..."
        else:
            null_class_list = ", ".join(class_stats.loc[null_stats, CLASS_COLUMN].tolist())
        LOGGER.warning(
            f"{null_stats.sum()} classes have null {x_field} and/or {y_field} and will not be plotted: {null_class_list}"
        )
        # points with a null value won't show up on the pyecharts scatterplot, but they do get used for setting the
        # bounds of the non-null dimension, so dropping them here prevents the bounds from being wonky
        class_stats = class_stats[~null_stats]

    tooltip_fields = class_stats.columns.tolist()  # grab these before adding the color_by column

    if color_by:
        assert color_by in class_stats.columns, "color_by must be 'class' or a column in context_info"
        if context_info is not None and (color_by in context_info.columns):
            # check the original column type b/c dtypes can get messed up during joins with missing values
            inferred_type = pd.api.types.infer_dtype(context_info[color_by])
        else:
            inferred_type = pd.api.types.infer_dtype(class_stats[color_by])

        assert inferred_type in COLOR_BY_TYPES, f"color_by dtype must be one of {COLOR_BY_TYPES}"
        # convert to strings because pyecharts is picky
        class_stats[COLOR_BY_COLUMN] = class_stats[color_by].astype(str)
        color_cats: list[str] = class_stats[COLOR_BY_COLUMN].unique().tolist()  # type: ignore
        color_cats.sort()
    else:
        color_cats = []

    # Convert the dataframe rows to native Python types so pyecharts' JSON
    # serialization does not choke on numpy/pandas scalar types.
    chart_data = [tuple(row) for row in class_stats.to_numpy(dtype=object)]

    return basic.scatterplot(
        data=chart_data,
        field_names=class_stats.columns.tolist(),
        x_field=x_field,
        y_field=y_field,
        title="Classification Performance",
        subtitle=f"N = {int(report_dict['macro avg']['support'])} | Accuracy = {round(report_dict['accuracy'], 3)}",
        color_field=COLOR_BY_COLUMN if color_by else None,
        color_values=color_cats if color_by else None,
        tooltip=tooltip_fields,
        zoom_orient="horizontal",
    )
