"""Dump/load helpers for pickled `sklearn`-compatible estimators.

These wrap the ML-import-free helpers in `thds.mllegos.io` with estimator-typed signatures and
logging. This module imports `sklearn.base` eagerly, which is why it lives under `sklegos`.
"""

import os
import typing as ty

from sklearn.base import BaseEstimator, TransformerMixin

from thds.core import source
from thds.core.log import getLogger

from .. import io
from .types import AnyEstimator

_LOGGER = getLogger(__name__)


def dump_model(
    model: AnyEstimator, f_stem: str = "model", *, out_dir: ty.Union[str, os.PathLike, None] = None
) -> source.Source:
    """Pickle a fitted estimator via `io.to_pickle_source` (which logs the serialized size)."""
    _LOGGER.info("Dumping model to disk")
    return io.to_pickle_source(model, f_stem, out_dir=out_dir)


def load_model(src: ty.Union[str, os.PathLike]) -> AnyEstimator:
    """Unpickle an estimator from `src`, requiring an `sklearn.base` type."""
    _LOGGER.info("Loading model from %s", src)
    model = io.load_pickle_source_typed(BaseEstimator, TransformerMixin, src=src)
    _LOGGER.info("Loaded model:\n%s", model)
    return model
