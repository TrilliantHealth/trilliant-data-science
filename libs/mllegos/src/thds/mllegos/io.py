"""Pickle <-> `thds.core.source.Source` helpers.

Deliberately free of ML-framework imports, so modules that must load on minimal installs can use
these without pulling in heavy dependencies. For estimator-typed dump/load wrappers, see
`thds.mllegos.sklegos.io`.
"""

import os
import pickle
import tempfile
import typing as ty
from datetime import datetime
from pathlib import Path

from thds.core import source
from thds.core.log import getLogger

_LOGGER = getLogger(__name__)

T = ty.TypeVar("T")


def to_pickle_source(
    data: ty.Any, f_stem: str, *, out_dir: ty.Union[str, os.PathLike, None] = None
) -> source.Source:
    """Pickle `data` to `{f_stem}_{YYYYmmdd_HHMMSS_ffffff}.pkl` and wrap the file as a Source.

    With `out_dir=None` (the default), the file is written into a fresh temporary directory, so
    names can never collide. Passing an explicit `out_dir` creates the directory if needed and
    raises FileExistsError on a same-microsecond collision. The serialized size is logged at INFO.
    """
    dir_ = Path(tempfile.mkdtemp()) if out_dir is None else Path(out_dir)
    dir_.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    fpath = dir_ / f"{f_stem}_{timestamp}.pkl"
    if fpath.exists():
        raise FileExistsError(f"Output file already exists: {fpath}")
    with open(fpath, "wb") as fp:
        pickle.dump(data, fp)
    _LOGGER.info("Pickled %s to %s (%.1f MB)", f_stem, fpath, fpath.stat().st_size / 2**20)
    return source.from_file(fpath)


def load_pickle_source(src: ty.Union[str, os.PathLike]) -> ty.Any:
    """Unpickle the file at `src` (any PathLike, e.g. a `Source`)."""
    with open(src, "rb") as file:
        return pickle.load(file)


def load_pickle_source_typed(*types: ty.Type[T], src: ty.Union[str, os.PathLike]) -> T:
    """Unpickle the file at `src` and require the value to be an instance of one of `types`;
    raise TypeError if not.

    For a single type, the checker infers exactly that type. For several, it infers their common
    supertype (their join, not a union - and never `Any`, so a wrong annotation at the callsite
    cannot pass vacuously); annotate the receiving variable when you need something more precise.
    PEP 604 unions and `typing.Union` objects are not accepted - pass the member types as separate
    arguments.
    """
    if not types:
        raise TypeError("at least one type is required")
    value = load_pickle_source(src)
    if not isinstance(value, types):
        raise TypeError(f"Expected one of {types} on unpickling from {src}, got {type(value)}")
    return value
