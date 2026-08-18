"""Declarative numeric ranges for hyperparameter search spaces.

The range types are frozen stdlib dataclasses over plain python scalars: hashable, picklable, and
dependency-free, so they are safe to embed in configuration objects that serve as memoization or
cache keys. Live `skopt.space` dimension objects are only built inside `build_skopt_space`, whose
scikit-optimize import is lazy (the `skopt` extra).
"""

import typing as ty
from dataclasses import dataclass


@dataclass(frozen=True)
class IntRange:
    lo: int
    hi: int


@dataclass(frozen=True)
class FloatRange:
    lo: float
    hi: float
    log_uniform: bool = False


Range = ty.Union[IntRange, FloatRange]


def build_skopt_space(space: ty.Mapping[str, Range]) -> ty.Dict[str, ty.Any]:
    """Materialize plain ranges into live `skopt.space` dimension objects.

    `IntRange` maps to `Integer(lo, hi)`; `FloatRange` maps to `Real(lo, hi)` with a log-uniform
    or uniform prior. Raises TypeError for any other value type - at build time, rather than deep
    inside a search run.
    """
    try:
        from skopt.space import Integer, Real
    except ImportError as e:
        raise ImportError(
            "scikit-optimize is required to build a skopt search space; "
            "install with `pip install 'thds.mllegos[skopt]'`."
        ) from e

    def _dimension(rng: Range) -> ty.Any:
        if isinstance(rng, IntRange):
            return Integer(rng.lo, rng.hi)
        if isinstance(rng, FloatRange):
            return Real(rng.lo, rng.hi, prior="log-uniform" if rng.log_uniform else "uniform")
        raise TypeError(f"unsupported search-space dimension type: {type(rng).__name__}")

    return {name: _dimension(rng) for name, rng in space.items()}
