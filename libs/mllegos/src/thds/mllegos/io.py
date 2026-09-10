"""Pickle, parquet, and JSON <-> `thds.core.source.Source` helpers.

Deliberately free of ML-framework imports, so modules that must load on minimal installs can use
these without pulling in heavy dependencies. pandas is imported only when `to_parquet_source` is
called, never at module import. For estimator-typed dump/load wrappers, see
`thds.mllegos.sklegos.io`.
"""

import json
import os
import pickle
import tempfile
import typing as ty
from datetime import datetime
from pathlib import Path

from thds.core import source
from thds.core.log import getLogger

if ty.TYPE_CHECKING:
    import pandas as pd

_LOGGER = getLogger(__name__)

T = ty.TypeVar("T")


def _prepare_path(f_stem: str, suffix: str, out_dir: "str | os.PathLike | None") -> Path:
    """Resolve `{f_stem}{suffix}` under `out_dir`, or under a fresh temporary directory.

    With `out_dir=None`, names can never collide. An explicit `out_dir` is created if needed;
    a path that already exists raises FileExistsError rather than overwrite.
    """
    dir_ = Path(tempfile.mkdtemp()) if out_dir is None else Path(out_dir)
    dir_.mkdir(parents=True, exist_ok=True)
    fpath = dir_ / f"{f_stem}{suffix}"
    if fpath.exists():
        raise FileExistsError(f"Output file already exists: {fpath}")
    return fpath


def to_pickle_source(
    data: ty.Any, f_stem: str, *, out_dir: ty.Union[str, os.PathLike, None] = None
) -> source.Source:
    """Pickle `data` to `{f_stem}_{YYYYmmdd_HHMMSS_ffffff}.pkl` and wrap the file as a Source.

    With `out_dir=None` (the default), the file is written into a fresh temporary directory, so
    names can never collide. Passing an explicit `out_dir` creates the directory if needed and
    raises FileExistsError on a same-microsecond collision. The serialized size is logged at INFO.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    fpath = _prepare_path(f"{f_stem}_{timestamp}", ".pkl", out_dir)
    with open(fpath, "wb") as fp:
        pickle.dump(data, fp)
    _LOGGER.info("Pickled %s to %s (%.1f MB)", f_stem, fpath, fpath.stat().st_size / 2**20)
    return source.from_file(fpath)


def to_parquet_source(
    data: "pd.DataFrame | pd.Series",
    f_stem: str,
    *,
    out_dir: "str | os.PathLike | None" = None,
) -> source.Source:
    """Write `data` to `{f_stem}.parquet` and wrap the file as a Source.

    A Series is converted to a one-column DataFrame, since parquet has no
    bare-column representation. Writing requires a parquet engine (pyarrow;
    the `parquet` extra) in the calling environment.

    With `out_dir=None` (the default), the file is written into a fresh temporary directory, so
    names can never collide. Passing an explicit `out_dir` creates the directory if needed and
    raises FileExistsError if the file already exists. The written size is logged at INFO.
    """
    import pandas as pd  # deferred so this module imports without pandas

    frame = data.to_frame() if isinstance(data, pd.Series) else data
    fpath = _prepare_path(f_stem, ".parquet", out_dir)
    frame.to_parquet(fpath)
    _LOGGER.info("Wrote %s to %s (%.1f MB)", f_stem, fpath, fpath.stat().st_size / 2**20)
    return source.from_file(fpath)


def to_json_source(
    data: ty.Any,
    f_stem: str,
    *,
    out_dir: "str | os.PathLike | None" = None,
    indent: int | None = None,
) -> source.Source:
    """Serialize `data` (anything `json.dump` accepts) to `{f_stem}.json` and wrap it as a Source.

    `out_dir` behaves as in `to_parquet_source`.
    """
    fpath = _prepare_path(f_stem, ".json", out_dir)
    with open(fpath, "w") as fp:
        json.dump(data, fp, indent=indent)
    return source.from_file(fpath)


def load_json_source(src: "str | os.PathLike") -> ty.Any:
    """Deserialize the JSON file at `src` (any PathLike, e.g. a `Source`)."""
    with open(src) as fp:
        return json.load(fp)


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
