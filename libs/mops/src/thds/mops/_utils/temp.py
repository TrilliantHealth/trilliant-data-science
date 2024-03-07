"""Utility to make returning files via their Paths less confusing in the application code."""
import typing as ty
from contextlib import contextmanager
from pathlib import Path

from thds.core import lazy, scope, tmp


@contextmanager
def _temp_dir() -> ty.Iterator[Path]:
    with tmp.temppath_same_fs() as p:
        p.mkdir()
        yield p


_FOREVER_SCOPE = scope.Scope("until_mops_exit")
_SINGLE_REMOTE_TMP_DIR = lazy.Lazy(lambda: _FOREVER_SCOPE.enter(_temp_dir()))
# there's really no obvious reason why you'd ever need more than one of these as long as
# you're giving your actual output files names, so we create one as a global for general
# use.


def new_tempdir() -> Path:
    return _FOREVER_SCOPE.enter(_temp_dir())


def tempdir() -> Path:
    """Lazily creates a global/shared temporary directory and returns it as a Path.

    The files will get cleaned up when the interpreter exits.
    """
    return _SINGLE_REMOTE_TMP_DIR()
