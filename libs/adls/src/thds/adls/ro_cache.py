import os
import platform
import shutil
import tempfile
import typing as ty
from pathlib import Path

from thds.core import log
from thds.core import types as ct

from .fqn import AdlsFqn
from .link import LinkType, link, set_read_only
from .md5 import hash_using, hashlib

# On our GitHub runners, we can't make hardlinks from /runner/home to where our stuff actually goes.
# So we special case this here for now; later we might move this general code to thds.core or something.
_RUNNER_WORK = Path("/runner/_work")
if os.getenv("CI") and _RUNNER_WORK.exists() and _RUNNER_WORK.is_dir():
    __home = _RUNNER_WORK
else:
    __home = Path.home()
_GLOBAL = __home / ".adls-md5-ro-cache"

_MAX_CACHE_KEY_LEN = 255  # safe on most local filesystems?
logger = log.getLogger(__name__)
_IS_MAC = platform.system() == "Darwin"

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
    return Path(fqn_str).absolute()


def link_or_copy(src: ct.StrOrPath, dest: ct.StrOrPath, link_opts: LinkOpts) -> LinkType:
    if link_opts:
        link_types = tuple() if isinstance(link_opts, bool) else link_opts
        link_success_type = link(src, dest, *link_types)
        if link_success_type:
            return link_success_type
        logger.warning(f"Unable to link {src} to {dest}; falling back to copy.")

    with tempfile.TemporaryDirectory() as dir:
        tmpfile = os.path.join(dir, "tmp")
        shutil.copyfile(src, tmpfile)
        shutil.move(tmpfile, dest)
        # atomic to the final destination as long as we're on the same filesystem.
    return ""


def from_cache_path_to_local(cache_path: ct.StrOrPath, local_path: ct.StrOrPath, link_opts: LinkOpts):
    set_read_only(cache_path)
    link_success_type = link_or_copy(cache_path, local_path, link_opts)
    if link_success_type in {"ref", ""}:
        # hard and soft links do not have their own permissions - they
        # share the read-only permissions of their target.  reflinks
        # and copies will not, so those do not need to be marked as
        # read-only since edits to them will not affect the original,
        # cached copy.
        os.chmod(local_path, 0o644)  # 0o644 == rw-r--r-- (user, group, all)


def from_local_path_to_cache(local_path: ct.StrOrPath, cache_path: ct.StrOrPath, link_opts: LinkOpts):
    link_or_copy(local_path, cache_path, link_opts)
    set_read_only(cache_path)
