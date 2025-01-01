"""Various assorted file-related utilities."""

import hashlib
import os
import resource
import shutil
import stat
import typing as ty
from contextlib import contextmanager
from io import BufferedWriter, TextIOWrapper
from pathlib import Path

from . import config, hashing
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
    str_path = remove_file_scheme(uri)
    if not str_path:
        raise ValueError('Cannot convert an empty string to a Path. Did you mean to use "."?')
    return Path(str_path)


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


@contextmanager
def atomic_text_writer(destination: StrOrPath) -> ty.Iterator[TextIOWrapper]:
    """Even shorter shorthand for writing text data to a file, atomically."""
    with atomic_write_path(destination) as temp_writable_path:
        with open(temp_writable_path, "w") as f:
            yield f


OPEN_FILES_LIMIT = config.item("limit_open", 10000)


def set_file_limit(n: int):
    """Works like calling `ulimit -Sn <N>` on a Mac."""
    resource.setrlimit(resource.RLIMIT_NOFILE, (n, n))
    assert resource.getrlimit(resource.RLIMIT_NOFILE) == (n, n)


def bump_limits():
    """It was common to have to do this manually on our macs. Now that is no longer required."""
    set_file_limit(OPEN_FILES_LIMIT())


def shorten_filename(maybe_too_long_name: StrOrPath, max_len: int = 255, retain_last: int = 30) -> str:
    """Shortens a filename, using a deterministic and probabilistically-unique (hash-based) algorithm.

    The filename is only changed if it exceeds the provided max_len limit.

    The limit defaults to 255 _bytes_ since that is what many filesystems have
    generally supported.  https://en.wikipedia.org/wiki/Comparison_of_file_systems#Limits

    We intentionally take our 'bite' out of the middle of the filename, so that the file extension is preserved
    and so that the first part of the path also remains human-readable.
    """
    # p for Path, s for str, b for bytes - too many things flying around to keep track of without this.
    s_maybe_too_long_name = Path(maybe_too_long_name).name
    b_filename = s_maybe_too_long_name.encode()

    if len(b_filename) <= max_len:
        # no need to mess with anything - it will 'fit' inside the root path already.
        return s_maybe_too_long_name

    b_md5_of_filename = (
        b"-md5-" + hashing.hash_using(b_filename, hashlib.md5()).hexdigest().encode() + b"-"
    )
    b_last_n = b_filename[-retain_last:]
    b_first_n = b_filename[: max_len - len(b_md5_of_filename) - len(b_last_n)]
    b_modified_filename = b_first_n + b_md5_of_filename + b_last_n
    assert len(b_modified_filename) <= max_len, (
        b_modified_filename,
        len(b_modified_filename),
        max_len,
    )
    return b_modified_filename.decode()
