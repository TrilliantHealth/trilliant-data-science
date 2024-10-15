import json
import typing as ty
from datetime import datetime, timezone

from thds.core import log

from ..types import BlobStore
from ..uris import lookup_blob_store

logger = log.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def write(blob_store: BlobStore, lock_uri: str, lock_bytes: bytes) -> None:
    try:
        blob_store.putbytes(lock_uri, lock_bytes, type_hint="application/mops-lock")
    except Exception:
        logger.error(f"Failed to write lock at {lock_uri}")
        raise


def json_dumpb(contents: ty.Mapping) -> bytes:
    return json.dumps(contents, indent=2).encode()


def store_and_lock_uri(lock_dir_uri: str) -> ty.Tuple[BlobStore, str]:
    blob_store = lookup_blob_store(lock_dir_uri)
    lock_uri = blob_store.join(lock_dir_uri, "lock.json")
    return blob_store, lock_uri


def make_lock_uri(lock_dir_uri: str) -> str:
    _, lock_uri = store_and_lock_uri(lock_dir_uri)
    return lock_uri
