# this module is for handling some new functionality related to using etags as a fallback
# for file hashing when the file properties do not include locally-verifiable hash information.
import typing as ty
from pathlib import Path

import xxhash

from thds.core import config, hash_cache, home, log, types

ETAG_FAKE_HASH_NAME = "adls-azure-etag-fake"
logger = log.getLogger(__name__)


def extract_etag_bytes(etag_str: str) -> bytes:
    # ADLS etags may or may not be quoted depending on the API used:
    # list_blobs returns unquoted, get_*_properties returns quoted.
    # Strip quotes first, then calculate byte length from the stripped string.
    stripped = etag_str.strip('"')
    return int(stripped, 16).to_bytes((len(stripped) - 2 + 1) // 2, byteorder="big")


_ETAG_CACHE = config.item("cache-path", home.HOMEDIR() / ".thds/adls/xxhash-onto-etag", parse=Path)


def add_to_etag_cache(local_path: types.StrOrPath, etag: bytes) -> hash_cache.Hash:
    xxh_bytes = hash_cache.hash_file(local_path, xxhash.xxh3_128())
    etag_path = _ETAG_CACHE() / xxh_bytes.hex()
    etag_path.parent.mkdir(parents=True, exist_ok=True)
    etag_path.write_bytes(etag)
    logger.debug("Writing etag 'hash' to path at %s", etag_path)
    return hash_cache.Hash(ETAG_FAKE_HASH_NAME, etag)


def hash_file_fake_etag(local_path: types.StrOrPath) -> ty.Optional[hash_cache.Hash]:
    try:
        xxh_bytes = hash_cache.hash_file(local_path, xxhash.xxh3_128())
    except FileNotFoundError:
        return None

    etag_path = _ETAG_CACHE() / xxh_bytes.hex()
    if etag_path.is_file():
        etag_bytes = etag_path.read_bytes()
        logger.debug("Reusing etag 'fake hash' from path at %s", etag_path)
        return hash_cache.Hash(ETAG_FAKE_HASH_NAME, etag_bytes)

    return None
