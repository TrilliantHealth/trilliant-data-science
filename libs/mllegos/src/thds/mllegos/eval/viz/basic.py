from __future__ import annotations

import warnings
from typing import Literal

import numpy as np
import pandas as pd

try:
    from pyecharts import options as opts
    from pyecharts.charts import Bar, Boxplot, Scatter
    from pyecharts.commons.utils import JsCode
except ImportError:
    warnings.warn(
        "pyecharts is not installed. Visualization functions in this module will not be usable. "
        "Install the pyecharts extra to enable visualization.",
        stacklevel=2,
    )

from . import utils

ZOOM_ORIENTATIONS = Literal["horizontal", "vertical"]


def scatterplot(
    data: list[tuple[str | int | float, ...]],
    field_names: list[str],
    x_field: str,
    y_field: str,
    title: str | None = None,
    subtitle: str | None = None,
    color_field: str | None = None,
    color_values: list[str] | None = None,
    tooltip: list[str] | None = None,
    zoom_orient: ZOOM_ORIENTATIONS | None = None,
) -> Scatter:
    """
    Make a pyecharts scatterplot with some optional features.

    Inputs:
        - data: chart data, formatted like a table without headers. Tip - from a pandas dataframe:
            `list(df.itertuples(index=False, name=None))`
        - field_names: list of column names in data; Tip - from a pandas dataframe `df.columns.tolist()`
        - x_field: field to be plotted on x axis
        - y_field: field to be plotted on y axis
        - title: optional chart title
        - subtitle: optional chart subtitle
        - color_field: field name of categorical data for coloring plot points; `color_values` must be provided
        - color_values: list of unique string values for coloring plot points; `color_field` must be provided
        - tooltip: optional list of fields to include in the tooltip
        - zoom_orient: optionally, add a zoom bar to the plot for either the x ('horizonal') or y ('vertical') axis

    Returns:
        - pyecharts Scatter chart
    """
    encoding: dict[str, str | list[str]] = {
        "x": x_field,
        "y": y_field,
    }
    if tooltip is not None:
        encoding["tooltip"] = tooltip

    # TODO: automatically get color values from data
    colormap = (
        utils.js_format_color(utils.create_color_map(color_values), field_names.index(color_field))
        if (color_field and color_values)
        else None
    )

    title_opts = (
        opts.TitleOpts(title=title, subtitle=subtitle, pos_left="50%", text_align="center")
        if (title is not None) | (subtitle is not None)
        else None
    )

    return (
        Scatter()
        .set_global_opts(
            datazoom_opts=(
                opts.DataZoomOpts(orient=zoom_orient, range_start=0, range_end=100)
                if zoom_orient
                else None
            ),
            xaxis_opts=opts.AxisOpts("value", name=x_field.title()),
            yaxis_opts=opts.AxisOpts("value", name=y_field.title()),
            title_opts=title_opts,
            toolbox_opts=opts.ToolboxOpts(
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(is_show=False),
                    data_view=opts.ToolBoxFeatureDataViewOpts(is_show=False),
                    magic_type=opts.ToolBoxFeatureMagicTypeOpts(is_show=False),
                )
            ),
        )
        .add_dataset(
            list(data),
            dimensions=field_names,
        )
        .add_yaxis(
            "",
            [],
            encode=encoding,
            itemstyle_opts=opts.ItemStyleOpts(color=JsCode(colormap)) if colormap else None,
            label_opts=opts.LabelOpts(is_show=False),
        )
    )


def barchart(
    data: list[tuple[str | int | float, ...]],
    field_names: list[str],
    numeric_field: str,
    categorical_field: str,
    title: str | None = None,
    subtitle: str | None = None,
    color_field: str | None = None,
    color_values: list[str] | None = None,
    tooltip: list[str] | None = None,
    zoom_end: int | None = 100,
) -> Bar:
    """
    Make a pyecharts scatterplot with some optional features.

    Inputs:
        - data: chart data, formatted like a table without headers. Tip - from a pandas dataframe:
            `list(df.itertuples(index=False, name=None))`
        - field_names: list of column names in data; Tip - from a pandas dataframe `df.columns.tolist()`
        - numeric_field: numeric field to be plotted on the x-axis (usually a count)
        - categorical_field: categorical field to be plotted on the y-axis
        - title: optional chart title
        - subtitle: optional chart subtitle
        - color_field: field name of categorical data for coloring bars; `color_values` must be provided
        - color_values: list of unique string values for coloring bars; `color_field` must be provided
        - tooltip: optional list of fields to include in the tooltip
        - zoom_orient: optionally, add a zoom bar to the plot for either the x ('horizonal') or y ('vertical') axis

    Returns:
        - pyecharts Bar chart
    """
    encoding: dict[str, str | list[str]] = {
        "x": numeric_field,
        "y": categorical_field,
    }
    if tooltip is not None:
        encoding["tooltip"] = tooltip

    # TODO: automatically get color values from data
    colormap = (
        utils.js_format_color(utils.create_color_map(color_values), field_names.index(color_field))
        if (color_field and color_values)
        else None
    )

    title_opts = (
        opts.TitleOpts(title=title, subtitle=subtitle, pos_left="50%", text_align="center")
        if (title is not None) | (subtitle is not None)
        else None
    )

    return (
        Bar()
        .set_global_opts(
            xaxis_opts=opts.AxisOpts("value", name=numeric_field, name_location="middle", name_gap=25),
            yaxis_opts=opts.AxisOpts(
                "category",
                is_inverse=True,
                name=categorical_field,
                name_gap=25,
                axislabel_opts=opts.LabelOpts(font_size=14),
            ),
            title_opts=title_opts,
            tooltip_opts=opts.TooltipOpts(extra_css_text="width:300px; white-space:pre-wrap;"),
            datazoom_opts=(
                opts.DataZoomOpts(orient="vertical", range_start=0, range_end=zoom_end)
                if zoom_end is not None
                else None
            ),
            toolbox_opts=opts.ToolboxOpts(
                feature=opts.ToolBoxFeatureOpts(
                    save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(is_show=False),
                    data_view=opts.ToolBoxFeatureDataViewOpts(is_show=False),
                    magic_type=opts.ToolBoxFeatureMagicTypeOpts(is_show=False),
                )
            ),
        )
        .add_dataset(data, dimensions=field_names)
        .add_yaxis(
            "",
            [],
            encode=encoding,
            itemstyle_opts=opts.ItemStyleOpts(color=JsCode(colormap)) if colormap else None,
        )
    )


def boxplot(data: pd.Series, color: str | None = None) -> Boxplot:
    """
    Create a pyecharts boxplot from a data distribution.
    - Box represents the interquartile range (25th to 75th percentile).
    - Whiskers represent the 10th and 90th percentiles.

    Inputs:
        - data: Series containing the distribution
        - color: optional color for the boxplot border

    Returns:
        - Boxplot chart object

    # TODO: make data more generalizable - could be any array
    """

    quantiles = [round(float(q), 3) for q in np.quantile(data.dropna(), [0.1, 0.25, 0.5, 0.75, 0.9])]

    return (
        Boxplot()
        .set_global_opts(
            xaxis_opts=opts.AxisOpts("category"),
            yaxis_opts=opts.AxisOpts("value"),
        )
        .add_xaxis([""])
        .add_yaxis(
            "",
            [quantiles],
            itemstyle_opts=opts.ItemStyleOpts(border_color=color),
            tooltip_opts=opts.TooltipOpts(
                formatter=JsCode(
                    """function(param) { return [
                                    '90th percentile: ' + param.data[5],
                                    '75th percentile: ' + param.data[4],
                                    'Median: ' + param.data[3],
                                    '25th percentile ' + param.data[2],
                                    '10th percentile: ' + param.data[1]
                                ].join('<br/>') }"""
                )
            ),
        )
    )
