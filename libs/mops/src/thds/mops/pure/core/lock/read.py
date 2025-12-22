import io
import json
import typing as ty

from thds.core import log

from ..control_cache import CONTROL_CACHE_TTL_IN_SECONDS
from ..uris import lookup_blob_store
from .types import LockContents

logger = log.getLogger(__name__)


def get_writer_id(lock_contents: LockContents) -> str:
    return lock_contents["writer_id"]


def make_read_lockfile(lock_uri: str) -> ty.Callable[[], ty.Optional[LockContents]]:
    def read_lockfile() -> ty.Optional[LockContents]:
        # A negative value results in the cache blob store not being used. The
        # important part is that this bypasses the hash check. This avoids a
        # race condition where the lockfile is overwritten by the local
        # runner after the remote runner reads the remote hash but _before_
        # it downloads the file, resulting in a `HashMismatchError`.
        with CONTROL_CACHE_TTL_IN_SECONDS.set_local(-1):
            blob_store = lookup_blob_store(lock_uri)

        while True:
            lockfile_bio = io.BytesIO()
            try:
                # NO OPTIMIZE: this read must never be optimized in any way.
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
