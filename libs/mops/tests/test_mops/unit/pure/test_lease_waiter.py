"""The shared lease waiter must resolve every future it is handed - with someone else's
result, with our own post-takeover result, or with an exception - and never block the
registering thread."""

import concurrent.futures
import pickle
import threading
import time
import typing as ty

import pytest

from thds.core import futures
from thds.mops.pure._futures import MopsFuture
from thds.mops.pure.runner import lease_waiter, same_process_in_flight


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr(lease_waiter, "_INITIAL_BACKOFF_S", 0.01)
    monkeypatch.setattr(lease_waiter, "_MAX_BACKOFF_S", 0.05)


class _FakeLease:
    writer_id = "test-writer"
    expire_s = 88.0

    def maintain(self) -> None:
        pass

    def release(self) -> None:
        pass


def _never_result() -> None:
    return None


def _no_lease() -> None:
    return None


def _no_unwrap(_result: ty.Any) -> ty.NoReturn:
    raise AssertionError("nothing to unwrap")


def _no_invoke(_lease: ty.Any) -> ty.NoReturn:
    raise AssertionError("should not have invoked")


def _resolved_mops_future(value: ty.Any) -> MopsFuture:
    f = MopsFuture(futures.resolved(value), "memo://unit/inner")
    f.set_result_metadata(None)
    return f


def test_resolves_when_someone_elses_result_appears():
    checks = []

    def check_result():
        checks.append(1)
        return "RESULT" if len(checks) >= 3 else None

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/other-result",
        what="result",
        check_result=check_result,
        unwrap=lambda r: (r.lower(), None),
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    assert not fut.done()  # registration never blocks on the wait
    assert fut.result(timeout=10) == ("result", None)
    assert len(checks) >= 3


def test_memoized_exception_is_raised_by_the_future():
    def raise_memoized(_result):
        raise ValueError("the memoized exception")

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/memoized-error",
        what="result",
        check_result=lambda: "ERROR",
        unwrap=raise_memoized,
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    with pytest.raises(ValueError, match="the memoized exception"):
        fut.result(timeout=10)


def test_takeover_dispatches_off_the_waiter_thread_and_bridges_the_result():
    invoked_on_threads = []

    def invoke(lease_owned):
        assert lease_owned.writer_id == "test-writer"
        invoked_on_threads.append(threading.current_thread().name)
        return _resolved_mops_future(42)

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/takeover",
        what="result",
        check_result=_never_result,
        unwrap=_no_unwrap,
        acquire_lease=_FakeLease,
        invoke_with_lease=invoke,
    )
    assert fut.result(timeout=10) == (42, None)
    assert invoked_on_threads == ["mops-lease-takeover"]


def test_takeover_invocation_error_fails_the_future():
    def invoke(_lease):
        raise RuntimeError("dispatch exploded")

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/takeover-error",
        what="result",
        check_result=_never_result,
        unwrap=_no_unwrap,
        acquire_lease=_FakeLease,
        invoke_with_lease=invoke,
    )
    with pytest.raises(RuntimeError, match="dispatch exploded"):
        fut.result(timeout=10)


def test_takeover_invocation_future_exception_is_bridged():
    def invoke(_lease):
        inner: concurrent.futures.Future = concurrent.futures.Future()
        inner.set_exception(RuntimeError("the function failed"))
        return MopsFuture(inner, "memo://unit/inner")

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/takeover-fn-error",
        what="result",
        check_result=_never_result,
        unwrap=_no_unwrap,
        acquire_lease=_FakeLease,
        invoke_with_lease=invoke,
    )
    with pytest.raises(RuntimeError, match="the function failed"):
        fut.result(timeout=10)


def test_error_while_polling_fails_the_future_instead_of_stranding_it():
    def check_result():
        raise ConnectionError("blob store unreachable")

    fut: concurrent.futures.Future = lease_waiter.future_awaiting_lease(
        "memo://unit/poll-error",
        what="result",
        check_result=check_result,
        unwrap=_no_unwrap,
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    with pytest.raises(ConnectionError):
        fut.result(timeout=10)


def test_cancelling_the_future_stops_the_polling():
    checks = []

    def check_result():
        checks.append(1)
        return None

    fut: concurrent.futures.Future = lease_waiter.future_awaiting_lease(
        "memo://unit/cancelled",
        what="result",
        check_result=check_result,
        unwrap=_no_unwrap,
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    assert fut.cancel()
    time.sleep(0.3)  # any in-flight poll finishes and the next one observes done()
    polls_after_cancel = len(checks)
    time.sleep(0.3)
    assert len(checks) == polls_after_cancel


def test_pickling_after_takeover_delegates_to_the_invocation_future():
    inner: concurrent.futures.Future = concurrent.futures.Future()  # yields (value, metadata)

    def invoke(_lease):
        return MopsFuture.from_tuple_future(inner, "memo://unit/takeover-pickle")

    fut: ty.Any = lease_waiter.future_awaiting_lease(
        "memo://unit/takeover-pickle",
        what="result",
        check_result=_never_result,
        unwrap=_no_unwrap,
        acquire_lease=_FakeLease,
        invoke_with_lease=invoke,
    )
    deadline = time.monotonic() + 10
    while fut._takeover_future is None:
        assert time.monotonic() < deadline, "takeover never happened"
        time.sleep(0.01)

    # the invocation is still running: reducing must NOT block, and must hand the
    # receiving process the (picklable, resumable) invocation future itself.
    reconstruct, args = fut.__reduce__()
    assert reconstruct is lease_waiter._unpickled
    assert args == (inner,)

    inner.set_result((99, None))
    assert fut.result(timeout=10) == (99, None)


def test_in_process_holder_settling_wakes_the_waiter_without_timed_polls(monkeypatch):
    # timed polling is disabled in all but name: only the in-process subscription made at
    # registration time can resolve the future within the assertion window.
    monkeypatch.setattr(lease_waiter, "_INITIAL_BACKOFF_S", 600.0)
    monkeypatch.setattr(lease_waiter, "_MAX_BACKOFF_S", 600.0)

    holder: concurrent.futures.Future = concurrent.futures.Future()
    same_process_in_flight.register("memo://unit/in-process-wake", holder)

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/in-process-wake",
        what="result",
        check_result=lambda: "RESULT" if holder.done() else None,
        unwrap=lambda r: (r.lower(), None),
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    assert not fut.done()

    holder.set_result(None)
    assert fut.result(timeout=10) == ("result", None)


class _SubscriptionObservableFuture(concurrent.futures.Future):
    """Records that some waiter subscribed, so the test can prove the re-peek happened
    before settling the holder."""

    def __init__(self) -> None:
        super().__init__()
        self.subscribed = threading.Event()

    def add_done_callback(self, fn: ty.Callable[["concurrent.futures.Future"], object]) -> None:
        self.subscribed.set()
        super().add_done_callback(fn)


def test_holder_registered_after_waiting_began_is_discovered_by_a_repeek(monkeypatch):
    monkeypatch.setattr(lease_waiter, "_INITIAL_BACKOFF_S", 0.01)
    monkeypatch.setattr(lease_waiter, "_MAX_BACKOFF_S", 0.05)

    holder = _SubscriptionObservableFuture()
    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/late-holder",
        what="result",
        check_result=lambda: "RESULT" if holder.done() else None,
        unwrap=lambda r: (r.upper(), None),
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    time.sleep(0.05)  # a few polls happen with nothing registered
    same_process_in_flight.register("memo://unit/late-holder", holder)

    # a subsequent poll re-peeks the registry and subscribes to the holder.
    assert holder.subscribed.wait(10), "no poll ever subscribed to the late holder"
    holder.set_result(None)
    assert fut.result(timeout=10) == ("RESULT", None)


def test_pickling_a_pending_future_blocks_until_resolution():
    result_ready = threading.Event()

    def check_result():
        return "RESULT" if result_ready.is_set() else None

    fut = lease_waiter.future_awaiting_lease(
        "memo://unit/pickled",
        what="result",
        check_result=check_result,
        unwrap=lambda r: (7, None),
        acquire_lease=_no_lease,
        invoke_with_lease=_no_invoke,
    )
    threading.Timer(0.1, result_ready.set).start()
    restored = pickle.loads(pickle.dumps(fut))  # blocks until the waiter resolves it
    assert restored.result(timeout=0) == (7, None)
