"""Sometimes, you just want to cache hashes. Specifically, hashes of files.

We cache these hashes as files themselves, and the default location is under the user's
home directory.

The name of the file is an implementation detail that includes the hash of the file path,
the directory it lives in is the hashlib name of the hash algorithm, and the contents of
the file are the raw bytes of the hash. However, none of these details is guaranteed to
remain stable over time, and the only stable interface is the `hash_file` and `filehash`
functions themselves.
"""

import hashlib
import os
import sys
from pathlib import Path
from typing import Any

from . import config, files
from .hashing import Hash, hash_using
from .home import HOMEDIR
from .log import getLogger
from .types import StrOrPath

CACHE_HASH_DIR = config.item("directory", HOMEDIR() / ".hash-cache", parse=Path)
_1GB = 1 * 2**30  # log if hashing a file larger than this, since it will be slow.


logger = getLogger(__name__)


def _filecachekey(path: Path, hashtype: str) -> Path:
    # the construction of our cache key here is somewhat arbitrary,
    # and the name substring is really just for debugging purposes.
    # however, the filesize is a useful bit of additional 'entropy'
    # that will help us avoid edge cases that might arise from race
    # conditions, and the approach must remain stable over time for
    # the cache to provide a meaningful advantage.
    path_str = str(path)
    path_hash = hash_using(path_str.encode(), hashlib.sha256()).hexdigest()
    # we use a compressed (hashed) version of the path because
    # filenames can get kind of long and we don't want to deal with
    # long filenames blowing up our system by being unwritable.
    return (
        CACHE_HASH_DIR()
        / hashtype
        / (path_str[-50:].replace("/", "|") + "-" + path_hash + "+" + str(path.stat().st_size))
    )


def _is_no_older_than(file: Path, other: Path) -> bool:
    """Returns True if `file` is no older than `other`. Both files must exist."""
    return file.stat().st_mtime >= other.stat().st_mtime


def hash_file(filepath: StrOrPath, hasher: Any) -> bytes:
    """Hashes a file with the given hashlib hasher. If we've already previously computed
    the given hash for the file and the file hasn't changed (according to filesystem
    mtime) since we stored that hash, we'll just return the cached hash.

    File must exist and respond positively to stat().
    """
    resolved_path = Path(filepath).resolve()
    cached_hash_path = _filecachekey(resolved_path, hasher.name)
    # now we can check to see if we have hash bytes for that file somewhere already.
    hash_cached = "hash-cached" if cached_hash_path.exists() else ""
    if hash_cached and _is_no_older_than(cached_hash_path, resolved_path):
        logger.debug("Reusing known hash for %s - cache key %s", resolved_path, cached_hash_path)
        return cached_hash_path.read_bytes()

    psize = resolved_path.stat().st_size
    if psize > _1GB:
        log_at_lvl = logger.warning if hash_cached else logger.info
        # I want to know how often we're finding 'outdated' hashes; those should be rare.
        log_at_lvl(f"Hashing {psize/_1GB:.2f} GB file at {resolved_path}{hash_cached}")

    hash_bytes = hash_using(resolved_path, hasher).digest()
    cached_hash_path.parent.mkdir(parents=True, exist_ok=True)
    with files.atomic_binary_writer(cached_hash_path) as f:
        f.write(hash_bytes)
    return hash_bytes


def filehash(algo: str, pathlike: os.PathLike) -> Hash:
    """Wraps a cached hash of a file in a core.hashing.Hash object, which carries the name
    of the hash algorithm used."""
    return Hash(sys.intern(algo), hash_file(pathlike, hashlib.new(algo)))
