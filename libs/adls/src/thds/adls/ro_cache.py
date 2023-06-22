import os
import platform
import shutil
import tempfile
import typing as ty
from pathlib import Path

from thds.core import log
from thds.core import types as ct

from .fqn import AdlsFqn
from .link import link, set_read_only
from .md5 import hash_using, hashlib

_GLOBAL = Path.home() / ".adls-md5-ro-cache"
_MAX_CACHE_KEY_LEN = 255  # safe on most local filesystems?
logger = log.getLogger(__name__)
_IS_MAC = platform.system() == "Darwin"


class Cache(ty.NamedTuple):
    """Immutable struct declaring what cache behavior is desired."""

    root: Path
    link: bool  # hard link is preferred, then symlink. Falls back to file copy.

    def path(self, fqn: AdlsFqn) -> Path:
        p = _cache_path_for_fqn(self, fqn)
        # we do not call this function anywhere unless we're planning
        # on downloading, so in general this should not create
        # empty directories that we didn't expect to use.
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


def global_cache(link: bool = True) -> Cache:
    """This is the recommended caching configuration."""
    return Cache(_GLOBAL, link)


def _cache_path_for_fqn(cache: Cache, fqn: AdlsFqn) -> Path:
    fqn_str = str(cache.root.resolve() / f"{fqn.sa}/{fqn.container}/{fqn.path}")
    fqn_bytes = fqn_str.encode()
    if len(fqn_bytes) > _MAX_CACHE_KEY_LEN:
        # we now hash the fqn itself, and overwrite the last N bytes
        # of the filename bytes with the hash. this gets us
        # consistency even across cache directories, such that the
        # cache directory is basically relocatable. It also makes testing easier.
        md5_of_key = b"-md5-" + hash_using(str(fqn).encode(), hashlib.md5()).hexdigest().encode()
        fqn_bytes = fqn_bytes[: _MAX_CACHE_KEY_LEN - len(md5_of_key)] + md5_of_key
        fqn_str = fqn_bytes.decode()
        assert len(fqn_bytes) <= _MAX_CACHE_KEY_LEN, (fqn_str, len(fqn_bytes))
    return Path(fqn_str).resolve()


def from_cache_path_to_local(cache_path: ct.StrOrPath, local_path: ct.StrOrPath, do_link: bool):
    set_read_only(cache_path)
    if do_link:
        # hard and soft links do not have their own permissions - they
        # share the read-only permissions of their target.  reflinks
        # and copies will not, but those do not need to be marked as
        # read-only since edits to them will not affect the original,
        # cached copy.
        link_type = link(cache_path, local_path)
        if link_type:
            return
        logger.warning(f"Unable to link {cache_path} to {local_path}; falling back to copy.")

    with tempfile.TemporaryDirectory() as dir:
        tmpfile = os.path.join(dir, "tmp")
        shutil.copyfile(cache_path, tmpfile)
        shutil.move(tmpfile, local_path)
        # atomic to the final destination as long as we're on the same filesystem.
