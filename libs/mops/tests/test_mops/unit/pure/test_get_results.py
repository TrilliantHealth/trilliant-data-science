"""The lease must be released no matter how the shim future ends. The result getter's
finally only covers the success path; failure and cancellation are covered by a done
callback on the shim future."""

import concurrent.futures
import pickle
from functools import partial

import pytest

from thds.core import futures
from thds.mops.pure.core.lease import maintain
from thds.mops.pure.core.types import NoResultAfterShimSuccess
from thds.mops.pure.runner.get_results import PostShimResultGetter, lease_maintaining_future


class _FakeLease:
    writer_id = "test-writer"
    expire_s = 88.0

    def __init__(self) -> None:
        self.released = 0

    def maintain(self) -> None:
        pass

    def release(self) -> None:
        self.released += 1


@pytest.fixture(autouse=True)
def _no_maintenance_daemon(monkeypatch):
    monkeypatch.setattr(maintain, "MAINTAIN_LEASES", lambda: False)


def _getter(memo_uri: str) -> PostShimResultGetter:
    return PostShimResultGetter(memo_uri, partial(_should_not_unwrap))


def _should_not_unwrap(*args):
    raise AssertionError("result unwrapping should not be reached in these tests")


def test_failed_shim_future_releases_the_lease():
    fake_lease = _FakeLease()
    shim_future: concurrent.futures.Future = concurrent.futures.Future()

    outer = lease_maintaining_future(fake_lease, _getter("memo://unit/failed-shim"), shim_future)
    assert fake_lease.released == 0

    shim_future.set_exception(RuntimeError("the Job failed"))
    with pytest.raises(RuntimeError, match="the Job failed"):
        outer.result(timeout=5)
    assert fake_lease.released == 1


def test_cancelled_shim_future_releases_the_lease():
    fake_lease = _FakeLease()
    shim_future: concurrent.futures.Future = concurrent.futures.Future()

    lease_maintaining_future(fake_lease, _getter("memo://unit/cancelled-shim"), shim_future)
    assert shim_future.cancel()
    assert fake_lease.released == 1


def _pending_forever() -> "concurrent.futures.Future":
    return concurrent.futures.Future()


def test_realized_future_with_release_callback_still_pickles():
    """mops futures cross process boundaries as LazyFutures, whose pickled state is only
    the mk_future - realized state, including the (unpicklable) release-on-failure
    callback this module attaches to the shim future, must stay out of the pickle."""
    fake_lease = _FakeLease()
    shim_future = futures.LazyFuture(_pending_forever)

    outer = futures.make_lazy(lease_maintaining_future)(
        fake_lease, _getter("memo://unit/pickle-realized"), shim_future
    )
    outer.add_done_callback(lambda f: None)  # realize: attaches the chain + release callback

    restored = pickle.loads(pickle.dumps(outer))
    assert isinstance(restored, futures.LazyFuture)

    # and the callback is still operative in the originating process:
    shim_future._lazy_future().set_exception(RuntimeError("job died after pickling"))
    assert fake_lease.released == 1


def test_realized_future_around_a_real_lease_pickles_with_maintenance_on(tmp_path):
    """A worker can race: something in-process (a same-process duplicate subscriber, a
    takeover) realizes the future chain before it is pickled back to the parent. The
    realized state then includes a real LeaseAcquired inside the maintenance daemon's
    release callable - all of which must still stay out of (or survive) the pickle."""
    from datetime import timedelta

    from thds.mops.pure.core import lease

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(maintain, "MAINTAIN_LEASES", lambda: True)  # undo the autouse fixture

    acquired = lease.acquire(f"file://{tmp_path}/lock", expire=timedelta(seconds=30))
    assert acquired
    shim_future = futures.LazyFuture(_pending_forever)
    outer = futures.make_lazy(lease_maintaining_future)(
        acquired, _getter("memo://unit/pickle-real-lease"), shim_future
    )
    outer.add_done_callback(lambda f: None)  # realize, as the raced worker would
    try:
        assert isinstance(pickle.loads(pickle.dumps(outer)), futures.LazyFuture)
    finally:
        acquired.release()
        monkeypatch.undo()


def test_successful_shim_future_releases_the_lease_exactly_once(monkeypatch):
    from thds.mops.pure.core import memo

    monkeypatch.setattr(memo.results, "check_if_result_exists", lambda *a, **kw: None)

    fake_lease = _FakeLease()
    shim_future: concurrent.futures.Future = concurrent.futures.Future()

    outer = lease_maintaining_future(fake_lease, _getter("memo://unit/ok-shim"), shim_future)
    shim_future.set_result(None)
    # the getter runs (and raises NoResultAfterShimSuccess since we stubbed no result),
    # exercising its finally-release; the failure callback must not double-release.
    with pytest.raises(NoResultAfterShimSuccess):
        outer.result(timeout=5)
    assert fake_lease.released == 1
