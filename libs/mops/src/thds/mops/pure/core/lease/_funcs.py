import json
import typing as ty
from datetime import datetime, timezone

from thds.core import log

from ..types import BlobStore
from ..uris import lookup_blob_store

logger = log.getLogger(__name__)

# These two names are on-storage paths shared with every other mops process that may
# coordinate on the same memo URI. They predate the lock->lease rename and must not
# change until a major version makes a clean break with pre-lease processes.
LEASE_DIRNAME = "lock"
_LEASEFILE_NAME = "lock.json"


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def write(blob_store: BlobStore, lease_uri: str, lease_bytes: bytes) -> None:
    try:
        blob_store.putbytes(lease_uri, lease_bytes, type_hint="application/mops-lease")
    except Exception:
        logger.error(f"Failed to write lease at {lease_uri}")
        raise


def json_dumpb(contents: ty.Mapping) -> bytes:
    return json.dumps(contents, indent=2).encode()


def store_and_lease_uri(lease_dir_uri: str) -> ty.Tuple[BlobStore, str]:
    blob_store = lookup_blob_store(lease_dir_uri)
    lease_uri = blob_store.join(lease_dir_uri, _LEASEFILE_NAME)
    return blob_store, lease_uri


def make_lease_uri(lease_dir_uri: str) -> str:
    _, lease_uri = store_and_lease_uri(lease_dir_uri)
    return lease_uri
