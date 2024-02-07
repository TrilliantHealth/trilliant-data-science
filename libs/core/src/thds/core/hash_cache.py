# sometimes, you just want to cache hashes. Specifically, hashes of files.
import hashlib
import os
from pathlib import Path
from typing import Any

from .config import ConfigItem
from .hashing import Hash, hash_using
from .home import HOMEDIR
from .log import getLogger
from .types import StrOrPath

CACHE_HASH_DIR = ConfigItem("directory", HOMEDIR() / ".hash-cache", parse=Path)
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


def hash_file(filepath: StrOrPath, hasher: Any) -> bytes:
    """Hashes a file with the given hashlib hasher. If we've already
    previously computed the given hash for the file and the file
    hasn't changed since we stored that hash, we'll just return the
    cached hash.

    File must exist and respond positively to stat().
    """
    resolved_path = Path(filepath).resolve()
    cached_hash_location = _filecachekey(resolved_path, hasher.name)
    # now we can check to see if we have hash bytes for that file somewhere already.
    if (
        cached_hash_location.exists()
        and cached_hash_location.stat().st_mtime >= resolved_path.stat().st_mtime
    ):
        logger.debug("Reusing known hash %s", resolved_path)
        return cached_hash_location.read_bytes()

    hash_bytes = hash_using(resolved_path, hasher).digest()
    cached_hash_location.parent.mkdir(parents=True, exist_ok=True)
    cached_hash_location.write_bytes(hash_bytes)
    return hash_bytes


def filehash(algo: str, pathlike: os.PathLike) -> Hash:
    return Hash(algo, hash_file(pathlike, hashlib.new(algo)))
