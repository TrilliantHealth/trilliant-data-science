"""Utilities for managing temporary directories and files.

Module is given this name partly because we tend not to name other things internally using
the word 'tmp', preferring instead 'temp'. Therefore `tmp` will be a little less ambiguous
overall.
"""

import contextlib
import shutil
import tempfile
import typing as ty
from pathlib import Path
from uuid import uuid4

from .home import HOMEDIR
from .types import StrOrPath


def _are_same_fs(path1: Path, path2: Path) -> bool:
    return path1.stat().st_dev == path2.stat().st_dev


def _walk_up_to_existing_dir(path: Path) -> Path:
    existing_dir = path.resolve()
    while not existing_dir.exists() or not existing_dir.is_dir():
        # this is guaranteed to terminate at the FS root
        existing_dir = existing_dir.parent
        if existing_dir.resolve() == existing_dir.parent.resolve():
            raise FileNotFoundError(f"{path} does not exist on a known filesystem.")
    return existing_dir


@contextlib.contextmanager
def temppath_same_fs(lcl_path: StrOrPath = "") -> ty.Iterator[Path]:
    """Builds a temporary Path (with no file at it) on the same filesystem as the
    provided local path. No file or directory is created for you at the actual
    Path.

    Useful for making sure that you can do atomic moves from tempfiles to your final
    location.
    """
    parent_dir = _walk_up_to_existing_dir(Path(lcl_path) if lcl_path else HOMEDIR())
    basename = Path(lcl_path).name

    def _tempdir_same_filesystem() -> ty.Iterator[Path]:
        # we would prefer _not_ to reinvent the wheel here, but if the tempfiles are
        # getting created on a different volume, then moves are no longer atomic, and
        # that's a huge pain for lots of reasons.
        fname_parts = [".core-tmp-home-fs", str(uuid4())]
        if basename:
            fname_parts.append(basename)
        dpath = parent_dir / "-".join(fname_parts)
        try:
            yield dpath
        finally:
            if dpath.exists():
                if dpath.is_file():
                    dpath.unlink()
                else:
                    shutil.rmtree(dpath, ignore_errors=True)

    with tempfile.TemporaryDirectory() as tdir:
        # actually check whether we're on the same filesystem.
        if _are_same_fs(parent_dir, Path(tdir)):
            # the standard path has us just using a normal temporary directory that we don't create ourselves.
            yield Path(tdir) / "-".join(filter(None, ["core-tmp", basename]))
        else:
            # but if we need to do something special... here we are.
            yield from _tempdir_same_filesystem()


@contextlib.contextmanager
def tempdir_same_fs(lcl_path: StrOrPath = "") -> ty.Iterator[Path]:
    """Builds a temporary directory on the same filesystem as the
    provided local path.

    Useful for making sure that you can do atomic moves from tempfiles to your final
    location.

    If you have no need for the above uses, teh built-in `tempfile.tmpdir` is a functionally
    equivalent substitute.
    """
    with temppath_same_fs(lcl_path) as tpath:
        tpath.mkdir(parents=True, exist_ok=True)
        yield tpath
