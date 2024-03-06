import os
import typing as ty
from pathlib import Path

from thds.core import config, log
from thds.core import types as ct
from thds.core.files import set_read_only
from thds.core.hashing import hash_using
from thds.core.home import HOMEDIR
from thds.core.link import LinkType, link_or_copy

from .fqn import AdlsFqn
from .md5 import hashlib

GLOBAL_CACHE_PATH = config.item("global-cache-path", HOMEDIR() / ".adls-md5-ro-cache", parse=Path)
MAX_CACHE_KEY_LEN = config.item("max-cache-key-len", 255, parse=int)  # safe on most local filesystems?
logger = log.getLogger(__name__)

LinkOpts = ty.Union[bool, ty.Tuple[LinkType, ...]]
# if True, order is reflink, hardlink, softlink, copy


class Cache(ty.NamedTuple):
    """Immutable struct declaring what cache behavior is desired."""

    root: Path
    link: LinkOpts

    def path(self, fqn: AdlsFqn) -> Path:
        p = _cache_path_for_fqn(self, fqn)
        # we do not call this function anywhere unless we're planning
        # on downloading, so in general this should not create
        # empty directories that we didn't expect to use.
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


def global_cache(link: LinkOpts = ("ref", "hard")) -> Cache:
    """This is the recommended caching configuration."""
    return Cache(GLOBAL_CACHE_PATH(), link)


def _cache_path_for_fqn(cache: Cache, fqn: AdlsFqn) -> Path:
    fqn_str = str(cache.root.resolve() / f"{fqn.sa}/{fqn.container}/{fqn.path}")
    fqn_bytes = fqn_str.encode()
    if len(fqn_bytes) > MAX_CACHE_KEY_LEN():
        # we now hash the fqn itself, and overwrite the last N bytes
        # of the filename bytes with the hash. this gets us
        # consistency even across cache directories, such that the
        # cache directory is basically relocatable. It also makes testing easier.
        md5_of_key = b"-md5-" + hash_using(str(fqn).encode(), hashlib.md5()).hexdigest().encode() + b"-"
        last_30 = fqn_bytes[-30:]
        first_n = fqn_bytes[: MAX_CACHE_KEY_LEN() - len(md5_of_key) - len(last_30)]
        fqn_bytes = first_n + md5_of_key + last_30
        fqn_str = fqn_bytes.decode()
        assert len(fqn_bytes) <= MAX_CACHE_KEY_LEN(), (fqn_str, len(fqn_bytes))
    return Path(fqn_str).absolute()


def _opts_to_types(opts: LinkOpts) -> ty.Tuple[LinkType, ...]:
    if opts is True:
        return ("ref", "hard")
    elif opts is False:
        return tuple()
    return opts


def from_cache_path_to_local(cache_path: ct.StrOrPath, local_path: ct.StrOrPath, link_opts: LinkOpts):
    set_read_only(cache_path)

    link_type = link_or_copy(cache_path, local_path, *_opts_to_types(link_opts))
    if link_type in {"ref", "", "same"}:
        # hard and soft links do not have their own permissions - they
        # share the read-only permissions of their target.  reflinks
        # and copies will not, so those should not be marked as
        # read-only since edits to them will not affect the original,
        # cached copy.
        os.chmod(local_path, 0o644)  # 0o644 == rw-r--r-- (user, group, all)


def from_local_path_to_cache(local_path: ct.StrOrPath, cache_path: ct.StrOrPath, link_opts: LinkOpts):
    link_or_copy(local_path, cache_path, *_opts_to_types(link_opts))
    set_read_only(cache_path)
