"""Best-effort to link a destination to a source depending on file system support."""
import filecmp
import os
import platform
import shutil
import subprocess
import tempfile
import typing as ty
from pathlib import Path

from . import log
from . import types as ct

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
        log_volume = (
            logger.warning if os.path.exists(dest) and os.path.getsize(dest) > 0 else logger.debug
        )
        os.remove(dest)  # link will fail if the path already exists
        log_volume('Removed existing file at "%s"', dest)
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


def reify_if_link(path: Path):
    """Turn a softlink to a target file into a copy of the target file at the link location.

    Useful for cases where a symlink crossing filesystems may not work
    as expected, e.g. a Docker build.

    No-op for anything that is not a symlink to a file.
    """
    if not path.is_symlink() or not path.is_file():
        return
    logger.info(f'Reifying softlink "{path}"')
    dest = path.absolute()
    src = path.resolve()
    dest.unlink()
    shutil.copy(src, dest)


def link_or_copy(src: ct.StrOrPath, dest: ct.StrOrPath, *link_types: LinkType) -> LinkType:
    if Path(src).exists() and Path(dest).exists() and filecmp.cmp(src, dest, shallow=False):
        # this filecmp operation may be somewhat expensive for large
        # files when they _are_ identical, but it's still better than
        # the race condition that exists with a file copy or a link
        # where the destination already exists.
        logger.debug("Destination %s for link is identical to source", dest)
        return "same"

    if link_types:
        link_success_type = link(src, dest, *link_types)
        if link_success_type:
            return link_success_type
        logger.warning(f"Unable to link {src} to {dest}; falling back to copy.")

    logger.debug("Copying %s to %s", src, dest)
    with tempfile.TemporaryDirectory(suffix="-linkcopy") as dir:
        tmpfile = os.path.join(dir, "tmp")
        shutil.copyfile(src, tmpfile)
        shutil.move(tmpfile, dest)
        # atomic to the final destination as long as we're on the same filesystem.
    return ""
