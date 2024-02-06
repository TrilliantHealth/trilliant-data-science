import os
import stat
from pathlib import Path

from .log import getLogger
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
