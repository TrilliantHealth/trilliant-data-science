"""A lease takeover must run the invocation inside a snapshot of the submitting
thread's context.

Regression test: a takeover thread used to serialize the invocation outside submit()'s
hashref context, writing an empty hashref header - so the remote side failed to unpickle
any Source argument ('source_from_hashref called without a hashref map context').
"""

import itertools
import threading
from datetime import datetime, timedelta

from thds.core.source import Source, from_file
from thds.mops import pure
from thds.mops.pure.core import lease
from thds.mops.pure.core.lease import maintain

from ...config import TEST_TMP_URI

_PIPELINE = f"test/takeover-context/{datetime.utcnow().isoformat()}"
_CALL_NUMBER = itertools.count()
_HOLDER_STARTED = threading.Event()
_RELEASE_HOLDER = threading.Event()


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE)
def _reads_source(src: Source) -> str:
    if next(_CALL_NUMBER) == 0:  # the first execution stalls, holding the lease
        _HOLDER_STARTED.set()
        assert _RELEASE_HOLDER.wait(90)
    with open(src) as f:
        return f.read()


def test_takeover_serializes_invocation_with_source_hashrefs(monkeypatch, tmp_path):
    # Shrink the lease so it expires while the first execution is stalled, and disable
    # both sides' maintenance so nothing refreshes it. The waiter's ~3s poll then wins
    # the takeover while the original holder is still running.
    real_acquire = lease.acquire
    monkeypatch.setattr(
        lease, "acquire", lambda uri, **kw: real_acquire(uri, expire=timedelta(seconds=2))
    )
    monkeypatch.setattr(maintain, "MAINTAIN_LEASES", lambda: False)
    monkeypatch.setattr(lease, "add_lease_to_maintenance_daemon", lambda writer: (lambda: None))

    src_file = tmp_path / "takeover-src.txt"
    src_file.write_text("takeover context works")

    holder = threading.Thread(target=_reads_source, args=(from_file(src_file),))
    holder.start()
    try:
        assert _HOLDER_STARTED.wait(30)

        fut = _reads_source.submit(from_file(src_file))
        assert not fut.done()  # genuinely lease-blocked behind the stalled holder
        assert fut.result(timeout=60) == "takeover context works"
    finally:
        _RELEASE_HOLDER.set()
        holder.join(60)
