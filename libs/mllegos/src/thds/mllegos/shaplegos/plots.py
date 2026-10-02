"""Thin wrappers over the standard `shap` figures.

Each accepts a `KeyedExplanation` (or, for `waterfall`, the single row `KeyedExplanation.row`
returns), never calls `plt.show()`, and returns the matplotlib `Figure` so a notebook can display
it and a script can save it. `fig` says where to draw: a `Figure`, an `Axes` within one, or `None`
for a fresh figure. `shap` draws on the *current* figure and axes, so each wrapper makes `fig`
current first - without that, consecutive plots in one script would pile onto each other. A
caller-supplied `fig` must be pyplot-managed (made with `plt.figure` or `plt.subplots`). Anything
not covered here is one `keyed.explanation` away from the full `shap.plots` API.
"""

import typing as ty

import shap
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from .explain import KeyedExplanation

PlotTarget = Axes | Figure | None


def _activate(fig: PlotTarget) -> Figure:
    """Make `fig` the current figure (and axes, when given one) and return that figure."""
    if fig is None:
        return plt.figure()
    if isinstance(fig, Axes):
        plt.sca(fig)
        return plt.gcf()  # the root figure behind the axes, even inside a SubFigure
    plt.figure(fig)
    return fig


def beeswarm(
    keyed: KeyedExplanation, *, max_display: int = 20, fig: PlotTarget = None, **kwargs: ty.Any
) -> Figure:
    """Feature importance and direction in one figure: one dot per instance per feature.

    Features are ordered by mean |SHAP|; dot position is the SHAP value, dot color the feature
    value. Extra `kwargs` go to `shap.plots.beeswarm`.
    """
    figure = _activate(fig)
    shap.plots.beeswarm(keyed.explanation, max_display=max_display, show=False, **kwargs)
    return figure


def violin(
    keyed: KeyedExplanation, *, max_display: int = 20, fig: PlotTarget = None, **kwargs: ty.Any
) -> Figure:
    """The beeswarm's distributional cousin: a violin of SHAP values per feature.

    `plot_type="layered_violin"` (via `kwargs`) also encodes the feature value as color bands.
    Extra `kwargs` go to `shap.plots.violin`.
    """
    figure = _activate(fig)
    shap.plots.violin(keyed.explanation, max_display=max_display, show=False, **kwargs)
    return figure


def bar(
    keyed: KeyedExplanation, *, max_display: int = 20, fig: PlotTarget = None, **kwargs: ty.Any
) -> Figure:
    """Mean |SHAP| per feature as a bar chart (the picture of `mean_abs_shap`).

    Extra `kwargs` go to `shap.plots.bar`.
    """
    figure = _activate(fig)
    shap.plots.bar(keyed.explanation, max_display=max_display, show=False, **kwargs)
    return figure


def waterfall(
    row: shap.Explanation, *, max_display: int = 15, title: str | None = None, fig: PlotTarget = None
) -> Figure:
    """How one instance's features move the model from the base value to its output.

    `row` is a single-instance explanation, e.g. `keyed.row(customer_id=..., date=...)`.
    """
    figure = _activate(fig)
    shap.plots.waterfall(row, max_display=max_display, show=False)
    if title is not None:
        figure.suptitle(title)
    return figure
