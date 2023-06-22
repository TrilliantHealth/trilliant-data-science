"""Best-effort to link a destination to a source depending on file system support."""
import os
import platform
import stat
import subprocess
import typing as ty
from pathlib import Path

from thds.core import log
from thds.core import types as ct

_IS_MAC = platform.system() == "Darwin"
logger = log.getLogger(__name__)


LinkType = ty.Literal["same", "ref", "hard", "soft", ""]


def link(
    src: ct.StrOrPath,
    dest: ct.StrOrPath,
    *attempt_types: LinkType,
) -> LinkType:
    """Attempt reflink, then hardlink, then softlink.

    First removes any existing file at dest.

    Return a non-empty string of type LinkType if a link was successful.

    Return empty string if no link could be created.
    """
    if not attempt_types:
        attempt_types = ("ref", "hard", "soft")
    src = Path(src).resolve()
    if src == Path(dest).resolve():
        return "same"
    assert os.path.exists(src), f"Source {src} does not exist"
    try:
        os.remove(dest)  # link will fail if the path already exists
        logger.warning(f'Removed existing file at "{dest}"')
    except FileNotFoundError:
        pass
    if _IS_MAC and "ref" in attempt_types:
        try:
            subprocess.check_output(["cp", "-c", str(src), str(dest)])
            logger.info(f"Created a copy-on-write reflink from {src} to {dest}")
            return "ref"
        except subprocess.CalledProcessError:
            pass
    if "hard" in attempt_types:
        try:
            os.link(src, dest)
            logger.info(f"Created a hardlink from {src} to {dest}")
            return "hard"
        except OSError as oserr:
            logger.warning(f"Unable to hard-link {src} to {dest} ({oserr})")
    if "soft" in attempt_types:
        try:
            os.symlink(src, dest)
            assert os.path.exists(dest), dest
            logger.info(f"Created a softlink from {src} to {dest}")
            return "soft"
        except OSError as oserr:
            logger.warning(f"Unable to soft-link {src} to {dest}" f" ({oserr})")

    return ""


def set_read_only(fpath: ct.StrOrPath):
    # thank you https://stackoverflow.com/a/51262451
    perms = stat.S_IMODE(os.lstat(fpath).st_mode)
    ro_mask = 0o777 ^ (stat.S_IWRITE | stat.S_IWGRP | stat.S_IWOTH)
    os.chmod(fpath, perms & ro_mask)
