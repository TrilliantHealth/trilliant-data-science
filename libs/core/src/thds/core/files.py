import os
import shutil
import stat
import typing as ty
from contextlib import contextmanager
from io import BufferedWriter
from pathlib import Path

from .log import getLogger
from .tmp import temppath_same_fs
from .types import StrOrPath

FILE_SCHEME = "file://"
logger = getLogger(__name__)


def set_read_only(fpath: StrOrPath):
    # thank you https://stackoverflow.com/a/51262451
    logger.debug("Setting '%s' to read-only", fpath)
    perms = stat.S_IMODE(os.lstat(fpath).st_mode)
    ro_mask = 0o777 ^ (stat.S_IWRITE | stat.S_IWGRP | stat.S_IWOTH)
    os.chmod(fpath, perms & ro_mask)


def remove_file_scheme(uri: str) -> str:
    """Does not require the file scheme to exist, but removes it if it's there."""
    return uri[len(FILE_SCHEME) :] if uri.startswith(FILE_SCHEME) else uri


def path_from_uri(uri: str) -> Path:
    return Path(remove_file_scheme(uri))


def to_uri(path: Path) -> str:
    return FILE_SCHEME + os.fspath(path.resolve())


def is_file_uri(uri: str) -> bool:
    return uri.startswith(FILE_SCHEME)


@contextmanager
def atomic_write_path(destination: StrOrPath) -> ty.Iterator[Path]:
    """Shorthand context manager for doing an atomic write (i.e., write to a temporary file,
    then atomically move that temporary file to your final destination.

    You must open and then close the file within the provided context. Unclosed files
    will likely result in data loss or other bugs.
    """
    destpath = path_from_uri(destination) if isinstance(destination, str) else Path(destination)
    with temppath_same_fs(destpath) as temp_writable_path:
        yield temp_writable_path
        destpath.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(temp_writable_path), destpath)


@contextmanager
def atomic_binary_writer(destination: StrOrPath) -> ty.Iterator[BufferedWriter]:
    """Even shorter shorthand for writing binary data to a file, atomically."""
    with atomic_write_path(destination) as temp_writable_path:
        with open(temp_writable_path, "wb") as f:
            yield f
