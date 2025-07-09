import os
import sys
import typing as ty
from pathlib import Path

from thds.core import config, log
from thds.core import types as ct
from thds.core.files import set_read_only
from thds.core.home import HOMEDIR
from thds.core.link import LinkType, link_or_copy

from .fqn import AdlsFqn
from .md5 import hex_md5_str

GLOBAL_CACHE_PATH = config.item("global-cache-path", HOMEDIR() / ".thds/adls/ro-cache", parse=Path)
MAX_FILENAME_LEN = config.item("max-filename-len", 255, parse=int)  # safe on most local filesystems?
MAX_TOTAL_PATH_LEN = config.item(
    "max-total-path-len", 1023 if sys.platform == "darwin" else 4095, parse=int
)
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
    return Cache(GLOBAL_CACHE_PATH(), link)


def _compress_long_path_part(part: str, max_bytes: int) -> str:
    md5_of_entire_part = "-md5-" + hex_md5_str(part) + "-"
    start_of_excised_section = (len(part) - len(md5_of_entire_part)) // 2
    end_of_excised_section = start_of_excised_section + len(md5_of_entire_part)

    while True:
        compressed_part = (
            part[:start_of_excised_section] + md5_of_entire_part + part[end_of_excised_section:]
        )
        num_bytes_overage = len(compressed_part.encode()) - max_bytes
        if num_bytes_overage <= 0:
            return compressed_part

        if len(part) - end_of_excised_section < start_of_excised_section:
            start_of_excised_section -= 1
        else:
            end_of_excised_section += 1
        # this is a very naive iterative approach to taking more 'bites' out of the middle of the filename.
        # we can't easily reason about how many bytes each character is, but we also can't
        # operate at the byte level directly, because removing bytes out of Unicode characters
        # will inevitably lead to invalid UTF-8 sequences.

        assert start_of_excised_section >= 0, (
            part,
            compressed_part,
            start_of_excised_section,
        )
        assert end_of_excised_section <= len(part), (
            part,
            compressed_part,
            end_of_excised_section,
        )


def _cache_path_for_fqn(cache: Cache, fqn: AdlsFqn) -> Path:
    """
    On Linux, file paths can be 255 bytes per part, and the max full path limit is
    4095, not including the NULL terminator.  On Mac, the max total length is 1023, and
    the max part length is 255.
    """
    # we assume that neither the SA nor the container will ever be more than MAX_FILENAME_LEN bytes.
    # However, we know that sometimes the path parts _are_, so in rare
    # cases we need unique yet mostly readable abbreviation for those.
    parts = list()
    for part in fqn.path.split("/"):
        part_bytes = part.encode()
        if len(part_bytes) > MAX_FILENAME_LEN():
            part = _compress_long_path_part(part, MAX_FILENAME_LEN())
        parts.append(part)

    full_path = str(Path(cache.root.resolve() / fqn.sa / fqn.container, *parts))
    if len(full_path.encode()) > MAX_TOTAL_PATH_LEN():
        full_path = _compress_long_path_part(full_path, MAX_TOTAL_PATH_LEN())

    return Path(full_path)


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
