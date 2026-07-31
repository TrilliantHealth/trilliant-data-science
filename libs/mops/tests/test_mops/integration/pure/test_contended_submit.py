"""A lease-blocked submit() returns a pending future instead of parking the thread until
the lease holder finishes."""

import concurrent.futures
import multiprocessing
import os
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

from thds.mops import pure

from ...config import TEST_TMP_URI

_PIPELINE = f"test/contended-submit/{datetime.utcnow().isoformat()}"
_STARTED = threading.Event()
_RELEASE = threading.Event()


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE)
def _blocks_until_released(x: int) -> int:
    _STARTED.set()
    assert _RELEASE.wait(30)
    return x


def test_contended_submit_returns_pending_future_and_resolves():
    # the samethread shim runs the function body on this thread, holding the lease the
    # whole time - so the duplicate submit below is genuinely lease-blocked.
    first_caller = threading.Thread(target=_blocks_until_released, args=(41,))
    first_caller.start()
    assert _STARTED.wait(30)

    before_submit = time.monotonic()
    fut = _blocks_until_released.submit(41)
    assert time.monotonic() - before_submit < 5  # pre-3.25, this blocked until the result existed
    assert not fut.done()

    _RELEASE.set()
    assert fut.result(timeout=30) == 41
    first_caller.join(30)


# ---------------------------------------------------------------------------
# Some orchestrators create futures in spawn'd process-pool workers and pickle them
# back to the parent, which consumes them via as_completed. A pending lease-blocked
# future cannot cross that boundary as-is (its waiter daemon dies with the worker),
# so pickling one must block until it resolves - the pre-3.25 behavior.
# ---------------------------------------------------------------------------

# a spawn'd worker re-imports this module, so the blob root must be identical in both
# processes: the parent mints it once and the child inherits it via os.environ.
_SHARED_ROOT_ENV = "MOPS_CONTENDED_PICKLE_TEST_ROOT"
if _SHARED_ROOT_ENV not in os.environ:
    os.environ[_SHARED_ROOT_ENV] = tempfile.mkdtemp(prefix="mops-contended-pickle-")
_SHARED_ROOT = Path(os.environ[_SHARED_ROOT_ENV])

_HELD_STARTED = threading.Event()
_HELD_RELEASE = threading.Event()


@pure.magic(blob_root=f"file://{_SHARED_ROOT / 'blob-root'}", pipeline_id="test/contended-pickle")
def _held_in_parent(x: int) -> int:
    _HELD_STARTED.set()
    assert _HELD_RELEASE.wait(60)
    return x * 2


def _submit_in_worker(x: int, sentinel: str) -> pure.MopsFuture:
    fut = _held_in_parent.submit(x)
    assert not fut.done()  # genuinely lease-blocked; the parent holds the lease.
    Path(sentinel).touch()
    return fut  # pickled on return - must block until the future resolves.


def test_lease_blocked_future_survives_worker_to_parent_pickling():
    first_caller = threading.Thread(target=_held_in_parent, args=(21,))
    first_caller.start()
    assert _HELD_STARTED.wait(30)

    sentinel = _SHARED_ROOT / "worker-has-pending-future"
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=1, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        pool_future = pool.submit(_submit_in_worker, 21, str(sentinel))

        deadline = time.monotonic() + 60
        while not sentinel.exists():
            assert time.monotonic() < deadline, "worker never produced a pending future"
            assert not pool_future.done(), pool_future.exception()
            time.sleep(0.05)

        _HELD_RELEASE.set()
        mops_future = pool_future.result(timeout=90)

    assert mops_future.result(timeout=5) == 42
    first_caller.join(30)
