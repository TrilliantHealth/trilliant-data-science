"""pyecharts visualizations for classifier evaluation."""

import typing as ty
import warnings

try:
    from pyecharts import options as opts
    from pyecharts.charts import HeatMap
    from pyecharts.commons.utils import JsCode
except ImportError:
    warnings.warn(
        "pyecharts is not installed. Visualization functions in this module will not be usable. "
        "Install the pyecharts extra to enable visualization.",
        stacklevel=2,
    )


class ConfusionMatrix(ty.TypedDict):
    tp: int
    fp: int
    fn: int
    tn: int


def confusion_matrix_heatmap(
    stats: ConfusionMatrix,
    *,
    unit_label: str = "records",
    range_color: tuple[str, str] | None = None,
) -> "HeatMap":
    """Build a binary confusion-matrix heatmap (Predicted x Actual, cells labeled with
    the percentage of total).

    :param stats: tp/fp/fn/tn counts.
    :param unit_label: what one count represents, shown in the cell tooltip.
    :param range_color: optional (low, high) cell background colors; pyecharts defaults
        when omitted.
    """
    counts = (stats["tp"], stats["fp"], stats["fn"], stats["tn"])
    total = sum(counts)

    # Data format: [x_index, y_index, value]
    data = [
        ["True", "False", stats["fp"]],
        ["False", "False", stats["tn"]],
        ["True", "True", stats["tp"]],
        ["False", "True", stats["fn"]],
    ]

    tooltip_formatter = JsCode(
        f"""
        function(params) {{
            return '{unit_label}: ' + params.data[2];
        }}
        """
    )
    display_percent = JsCode(
        f"""
        function(params) {{
            var total = {total};
            var pct = (params.data[2] / total * 100).toFixed(1);
            if (params.data[2] === 0.0) {{
                return '';
            }}
            return pct + '%';
        }}
        """
    )

    return (
        HeatMap()
        .add_xaxis(["True", "False"])
        .add_yaxis(
            "",
            ["False", "True"],
            data,
            label_opts=opts.LabelOpts(
                is_show=True,
                formatter=display_percent,
                font_size=14,
            ),
        )
        .set_global_opts(
            visualmap_opts=opts.VisualMapOpts(
                min_=0,
                max_=max(counts),
                is_show=False,
                range_color=list(range_color) if range_color else None,
            ),
            tooltip_opts=opts.TooltipOpts(formatter=tooltip_formatter),
            xaxis_opts=opts.AxisOpts(
                name="Predicted",
                name_location="center",
                name_gap=30,
                type_="category",
            ),
            yaxis_opts=opts.AxisOpts(
                name="Actual",
                name_location="center",
                name_gap=40,
                type_="category",
            ),
        )
    )
