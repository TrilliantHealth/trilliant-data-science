"""SHAP explanations for fitted models over named pandas feature frames.

The legos here move keyed pandas data into the shape `shap` wants and back out again:

* `explain_tree` runs `shap.TreeExplainer` over a feature frame and returns a `KeyedExplanation`,
  a `shap.Explanation` whose rows are addressable by domain keys (e.g. a customer id and a date).
* `mean_abs_shap` and `row_contributions` tabulate global and per-instance feature attributions.
* `plots` wraps the standard `shap` figures (beeswarm, violin, bar, waterfall) so they accept a
  `KeyedExplanation` and return the matplotlib `Figure` instead of showing it.

Requires the `shap` extra (`shap` + `matplotlib`).
"""

from . import plots
from .explain import (
    KeyedExplanation,
    RowContributions,
    explain_tree,
    mean_abs_shap,
    row_contributions,
    to_float_frame,
)

__all__ = [
    "KeyedExplanation",
    "RowContributions",
    "explain_tree",
    "mean_abs_shap",
    "plots",
    "row_contributions",
    "to_float_frame",
]
