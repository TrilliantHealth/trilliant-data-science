"""Reading a lease without participating in it.

The lease exists so that exactly one process works on an invocation at a time, and the
rest of this package is written for the processes doing that work. A tool that only wants
to *look* - to answer "is anything still working on this?" - needs the contents and none
of the machinery, which is what this provides.

That question has no other answer. When a remote dies without reporting, its events simply
stop, which is indistinguishable from a remote that is still running. The lease is the
only thing that keeps being refreshed, so its age is the liveness signal.
"""

import typing as ty

from thds.core import log

from ..uris import lookup_blob_store
from . import _funcs, read
from .types import LeaseContents

logger = log.getLogger(__name__)


def lease_uri_for(memo_uri: str) -> str:
    """Where an invocation's lease lives, derived rather than recorded."""
    return _funcs.make_lease_uri(lookup_blob_store(memo_uri).join(memo_uri, _funcs.LEASE_DIRNAME))


def read_lease(memo_uri: str) -> ty.Optional[LeaseContents]:
    """The lease held on one invocation, or None if there is none to read.

    Never raises: a caller asking about liveness is diagnosing something, and should not
    have to handle an exception to be told "no information available".
    """
    try:
        return read.make_read_leasefile(lease_uri_for(memo_uri))()
    except Exception:
        logger.debug("Could not read the lease for %s", memo_uri, exc_info=True)
        return None
