"""Best-effort to link a destination to a source depending on file system support."""
import os
import platform
import shutil
import subprocess
import typing as ty
from pathlib import Path

from . import log, tmp
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

    The destination directory must already exist.

    Return a non-empty string of type LinkType if a link was successful.

    Return empty string if no link could be created.
    """
    if not attempt_types:
        attempt_types = ("ref", "hard", "soft")
    src = Path(src).resolve()
    if src == Path(dest).resolve():
        return "same"
    assert os.path.exists(src), f"Source {src} does not exist"

    dest_parent = Path(dest).parent
    if not os.path.exists(dest_parent):
        raise FileNotFoundError(f"Destination directory {dest_parent} does not exist")

    with tmp.temppath_same_fs(dest_parent) as tmp_link_dest:
        # links will _fail_ if the destination already exists.
        # Therefore, instead of linking directly to the destination,
        # we always create the link at a temporary file on the same filesystem
        # as the true destination. Then, we take advantage of atomic moves
        # within the same filesystem, because moves of links are themselves atomic!
        # https://unix.stackexchange.com/a/81900
        assert not tmp_link_dest.exists(), tmp_link_dest
        if _IS_MAC and "ref" in attempt_types:
            try:
                subprocess.check_output(["cp", "-c", str(src), str(tmp_link_dest)])
                os.rename(tmp_link_dest, dest)
                logger.info(f"Created a copy-on-write reflink from {src} to {dest}")
                return "ref"
            except subprocess.CalledProcessError:
                pass
        if "hard" in attempt_types:
            try:
                os.link(src, tmp_link_dest)
                os.rename(tmp_link_dest, dest)
                logger.info(f"Created a hardlink from {src} to {dest}")
                return "hard"
            except OSError as oserr:
                logger.warning(f"Unable to hard-link {src} to {dest} ({oserr})")
        if "soft" in attempt_types:
            try:
                os.symlink(src, tmp_link_dest)
                os.rename(tmp_link_dest, dest)
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
    """If you absolutely have to get your file to its destination, you should use this
    over link(), which could theoretically fail under certain conditions.
    """
    if link_types:
        link_success_type = link(src, dest, *link_types)
        if link_success_type:
            return link_success_type
        logger.warning(f"Unable to link {src} to {dest}; falling back to copy.")

    logger.debug("Copying %s to %s", src, dest)
    with tmp.temppath_same_fs(dest) as tmpfile:
        # atomic to the final destination since we're on the same filesystem.
        shutil.copyfile(src, tmpfile)
        shutil.move(str(tmpfile), dest)
    return ""
