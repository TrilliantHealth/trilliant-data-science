import io
import json
import typing as ty

from thds.core import log

from ..uris import lookup_blob_store
from .types import LockContents

logger = log.getLogger(__name__)


def make_read_lockfile(lock_uri: str) -> ty.Callable[[], ty.Optional[LockContents]]:
    blob_store = lookup_blob_store(lock_uri)

    def read_lockfile() -> ty.Optional[LockContents]:
        while True:
            lockfile_bio = io.BytesIO()
            try:
                blob_store.readbytesinto(lock_uri, lockfile_bio, type_hint="lock")
            except Exception as e:
                if blob_store.is_blob_not_found(e):
                    return None
                logger.error(f"Failed on {lock_uri}: {e}")
                raise

            if lockfile_bio.tell() == 0:  # nothing was written
                logger.debug("Lockfile %s was empty - retrying read.", lock_uri)
                continue
            return json.loads(lockfile_bio.getvalue().decode())

    return read_lockfile
