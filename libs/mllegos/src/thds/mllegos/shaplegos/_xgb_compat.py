"""Let shap < 0.50 read models written by xgboost >= 3.1.

xgboost 3.1 started serializing `learner_model_param.base_score` as a vector literal (`"[5E-1]"`)
instead of a scalar. shap 0.50 parses either form, but shap 0.50 also dropped Python 3.10, so on
3.10 we are held at 0.49, whose XGBoost loader does `float("[5E-1]")` and raises.

The loader decodes the model's UBJSON dump through one module-level function; we wrap that function
to rewrite a vector `base_score` back to the scalar string the old parser expects. A binary or
regression model writes a one-element vector. A multiclass model writes one element per class, and
shap 0.50 keeps only the first, so this shim does the same and the two versions explain a given
model identically. The patch is a no-op on shap >= 0.50. Delete this module once every consumer runs
Python >= 3.11.
"""

import ast
import functools
import typing as ty

import shap
from packaging import version

_FIXED_IN = version.parse("0.50")


def _scalar_base_score(learner_model_param: dict[str, ty.Any]) -> dict[str, ty.Any]:
    raw = learner_model_param.get("base_score")
    if not (isinstance(raw, str) and raw.lstrip().startswith("[")):
        return learner_model_param
    parsed = ast.literal_eval(raw)
    if isinstance(parsed, (list, tuple)) and parsed:
        # the first element, as shap 0.50's loader does for a per-class vector
        return {**learner_model_param, "base_score": repr(float(parsed[0]))}
    return learner_model_param


@functools.cache
def ensure_xgboost_base_score_compat() -> bool:
    """Install the decoder wrapper once; return whether it was needed."""
    if version.parse(shap.__version__) >= _FIXED_IN:
        return False
    from shap.explainers import _tree

    original = _tree.decode_ubjson_buffer

    @functools.wraps(original)
    def decode(fd: ty.Any) -> ty.Any:
        model = original(fd)
        learner = model.get("learner")
        if not (isinstance(learner, dict) and "learner_model_param" in learner):
            return model
        params = _scalar_base_score(learner["learner_model_param"])
        return {**model, "learner": {**learner, "learner_model_param": params}}

    _tree.decode_ubjson_buffer = decode
    return True
