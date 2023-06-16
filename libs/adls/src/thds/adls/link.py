"""Best-effort to link a destination to a source depending on file system support."""
import contextlib
import os
import platform
import stat
import subprocess
import typing as ty

from thds.core import log
from thds.core import types as ct

_IS_MAC = platform.system() == "Darwin"
logger = log.getLogger(__name__)


LinkType = ty.Literal["same", "ref", "hard", "soft", ""]


def link(src: ct.StrOrPath, dest: ct.StrOrPath) -> LinkType:
    """Attempt reflink, then hardlink, then softlink.

    First removes any existing file at dest.

    Return a non-empty string of type LinkType if a link was successful.

    Return empty string if no link could be created.
    """
    if str(src) == str(dest):
        return "same"

    with contextlib.suppress(FileNotFoundError):
        os.remove(dest)  # link will fail if the path already exists
    if _IS_MAC:
        try:
            subprocess.check_output(["cp", "-c", str(src), str(dest)])
            return "ref"
        except subprocess.CalledProcessError:
            pass
    try:
        os.link(src, dest)
        return "hard"
    except OSError as oserr:
        logger.warning(f"Unable to hard-link {src} to {dest}" f" ({oserr}); falling back to soft link.")
    try:
        os.symlink(src, dest)
        return "soft"
    except OSError as oserr:
        logger.warning(f"Unable to soft-link {src} to {dest}" f" ({oserr})")

    return ""


def set_read_only(fpath: ct.StrOrPath):
    # thank you https://stackoverflow.com/a/51262451
    perms = stat.S_IMODE(os.lstat(fpath).st_mode)
    ro_mask = 0o777 ^ (stat.S_IWRITE | stat.S_IWGRP | stat.S_IWOTH)
    os.chmod(fpath, perms & ro_mask)
