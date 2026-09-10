"""Config-driven construction of sklearn feature encoders.

The conf classes are frozen stdlib dataclasses - hashable, picklable, and safe to embed in
larger configuration objects that feed memoization keys. `one_hot_encoder` and
`scaling_transformer` map them onto live (unfitted) sklearn transformers. Assembly policy
around the encoders - dtype casts, imputation steps, pass-through coercion, pipeline order -
deliberately stays with the caller: those choices are model-specific, the encoder specs are
not.
"""

import typing as ty
from dataclasses import dataclass
from functools import partial

import numpy as np
import numpy.typing as npt
from sklearn.base import TransformerMixin
from sklearn.preprocessing import (
    FunctionTransformer,
    MaxAbsScaler,
    MinMaxScaler,
    OneHotEncoder,
    RobustScaler,
    StandardScaler,
)

ContinuousScaling = ty.Literal["log1p", "standard", "robust", "min_max", "max_abs"]
"""Supported scaling strategies for continuous features.

- `log1p`: log(1 + x), useful for right-skewed distributions.
- `standard`: zero-mean, unit-variance; can be negative, destroys sparsity.
- `robust`: median/IQR-based; resilient to outliers, destroys sparsity.
- `min_max`: scale to [0, 1]; destroys sparsity.
- `max_abs`: scale by maximum absolute value; preserves sign and sparsity.

`None` (no scaling) is represented by `ContinuousFeatureConf.scaling = None`.
"""

OneHotUnknownHandling = ty.Literal["ignore", "error", "infrequent_if_exist"]


@dataclass(frozen=True)
class ContinuousFeatureConf:
    """Encoding spec for a continuous feature.

    :param scaling: Scaling strategy applied to the feature's values; see
        `ContinuousScaling`. `None` means no scaling - raw values pass through (the common
        case for booleans and pre-scaled values). Stateless scalings (`log1p`) just apply a
        function; stateful ones learn statistics during `fit`.
    :param fill_value: Value substituted for NaN before scaling, for callers whose encoder
        assembly includes an imputation step. `None` means no imputation.
    """

    scaling: ContinuousScaling | None = None
    fill_value: float | int | None = None

    @property
    def is_sparse(self) -> bool:
        return False


@dataclass(frozen=True)
class OneHotEncoderConf:
    """Encoding spec for a categorical feature, wrapping `sklearn.preprocessing.OneHotEncoder`.

    :param handle_unknown: Behavior on unseen categories at transform - `"ignore"` emits
        all-zeros, `"error"` raises, `"infrequent_if_exist"` maps them to the infrequent
        bucket when one exists.
    :param min_frequency: Categories with fewer training-set occurrences than this (or a
        smaller fraction, when a float) are grouped into an infrequent bucket. `None` keeps
        every observed category.
    :param sparse_output: Whether the encoder emits a sparse matrix.
    """

    handle_unknown: OneHotUnknownHandling = "ignore"
    min_frequency: int | float | None = None
    sparse_output: bool = True

    @property
    def is_sparse(self) -> bool:
        return self.sparse_output


def log1p_float(x: npt.ArrayLike) -> np.ndarray:
    """Apply log1p after casting to float.

    Accepts both int and float arrays - int arrays are cast to float first since they lack
    a native log1p method.
    """
    return np.log1p(np.asarray(x, dtype=float))


_SCALING_TO_TRANSFORMER: dict[ContinuousScaling, ty.Callable[[], TransformerMixin]] = {
    "log1p": partial(FunctionTransformer, log1p_float, feature_names_out="one-to-one"),
    "standard": StandardScaler,
    "robust": RobustScaler,
    "min_max": MinMaxScaler,
    "max_abs": MaxAbsScaler,
}


def scaling_transformer(scaling: ContinuousScaling) -> TransformerMixin:
    """Build the unfitted transformer for one scaling strategy."""
    return _SCALING_TO_TRANSFORMER[scaling]()


def one_hot_encoder(conf: OneHotEncoderConf, *, dtype: npt.DTypeLike | None = None) -> OneHotEncoder:
    """Build an unfitted `OneHotEncoder` from its conf.

    `dtype` overrides sklearn's default output dtype (e.g. `np.float32` to halve the memory
    of the encoded matrix); `None` keeps sklearn's default.
    """
    dtype_kwargs: dict[str, ty.Any] = {} if dtype is None else {"dtype": dtype}
    return OneHotEncoder(
        handle_unknown=conf.handle_unknown,
        min_frequency=conf.min_frequency,
        sparse_output=conf.sparse_output,
        **dtype_kwargs,
    )
