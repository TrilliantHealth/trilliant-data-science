"""A same-process duplicate submit is woken by the lease owner's completion, not by
timed storage polling. The samethread shim exercises the placeholder-signal path in
runner/local.py: the owner's real future doesn't exist until the computation is done."""

import threading
from datetime import datetime

from thds.mops import pure
from thds.mops.pure.runner import lease_waiter

from ...config import TEST_TMP_URI

_PIPELINE = f"test/in-process-wake/{datetime.utcnow().isoformat()}"
_STARTED = threading.Event()
_RELEASE = threading.Event()


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE)
def _holds_lease(x: int) -> int:
    _STARTED.set()
    assert _RELEASE.wait(60)
    return x + 1


def test_same_process_duplicate_wakes_on_owner_completion(monkeypatch):
    # timed polling is disabled in all but name: only the in-process subscription can
    # resolve the duplicate within the assertion window.
    monkeypatch.setattr(lease_waiter, "_INITIAL_BACKOFF_S", 600.0)
    monkeypatch.setattr(lease_waiter, "_MAX_BACKOFF_S", 600.0)

    first_caller = threading.Thread(target=_holds_lease, args=(41,))
    first_caller.start()
    assert _STARTED.wait(30)

    fut = _holds_lease.submit(41)
    assert not fut.done()

    _RELEASE.set()
    assert fut.result(timeout=30) == 42
    first_caller.join(30)
