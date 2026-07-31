import io
import json
import typing as ty

from thds.core import log

from ..control_cache import CONTROL_CACHE_TTL_IN_SECONDS
from ..uris import lookup_blob_store
from .types import LeaseContents

logger = log.getLogger(__name__)


def get_writer_id(lease_contents: LeaseContents) -> str:
    return lease_contents["writer_id"]


def make_read_leasefile(lease_uri: str) -> ty.Callable[[], ty.Optional[LeaseContents]]:
    def read_leasefile() -> ty.Optional[LeaseContents]:
        # A negative value results in the cache blob store not being used. The
        # important part is that this bypasses the hash check. This avoids a
        # race condition where the leasefile is overwritten by the local
        # runner after the remote runner reads the remote hash but _before_
        # it downloads the file, resulting in a `HashMismatchError`.
        with CONTROL_CACHE_TTL_IN_SECONDS.set_local(-1):
            blob_store = lookup_blob_store(lease_uri)

        while True:
            leasefile_bio = io.BytesIO()
            try:
                # NO OPTIMIZE: this read must never be optimized in any way.
                blob_store.readbytesinto(lease_uri, leasefile_bio, type_hint="lease")
            except Exception as e:
                if blob_store.is_blob_not_found(e):
                    return None
                logger.error(f"Failed on {lease_uri}: {e}")
                raise

            if leasefile_bio.tell() == 0:  # nothing was written
                logger.debug("Leasefile %s was empty - retrying read.", lease_uri)
                continue
            return json.loads(leasefile_bio.getvalue().decode())

    return read_leasefile
