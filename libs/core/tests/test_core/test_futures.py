import concurrent.futures
import typing as ty

from thds.core import futures


def test_chained_future_running_reflects_inner_not_sink():
    # A chained outer is constructed un-started and only set_result-ed when the
    # inner finishes. Before the inner is done, running() must report the
    # inner's state - not the (never-RUNNING) outer sink's. Regression for the
    # bug where a bare-Future outer reported running()==False mid-flight.
    inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    inner.set_running_or_notify_cancel()
    outer = futures.chain_futures(inner, futures.identity)

    assert inner.running()
    assert outer.running()
    assert not outer.done()


def test_chained_future_delivers_translated_result():
    inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    outer = futures.chain_futures(inner, lambda x: x + 1)

    inner.set_result(41)
    assert outer.result(timeout=1) == 42
    assert outer.done()
    assert not outer.running()


def test_chained_future_propagates_exception():
    inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    outer = futures.chain_futures(inner, futures.identity)

    inner.set_exception(ValueError("boom"))
    assert isinstance(outer.exception(timeout=1), ValueError)


def test_chained_future_is_a_concurrent_future():
    # as_completed and reify_future rely on the outer being a real
    # concurrent.futures.Future (they reach into _state/_condition/_waiters).
    inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    assert isinstance(futures.chain_futures(inner, futures.identity), concurrent.futures.Future)


def test_reify_future_passes_through_concurrent_future():
    f: concurrent.futures.Future[int] = concurrent.futures.Future()
    assert futures.reify_future(f) is f


def test_as_completed_yields_chained_results():
    inner_a: concurrent.futures.Future[int] = concurrent.futures.Future()
    inner_b: concurrent.futures.Future[int] = concurrent.futures.Future()
    chained = [
        futures.chain_futures(inner_a, futures.identity),
        futures.chain_futures(inner_b, futures.identity),
    ]

    inner_a.set_result(1)
    inner_b.set_result(2)
    assert sorted(f.result(timeout=1) for f in futures.as_completed(chained)) == [1, 2]


class _FakeCancellable(futures.PFuture[int]):
    """A future-like whose cancel() returns a fixed tri-state value. Implements
    the PFuture surface (so it composes with chain_futures / LazyFuture) but
    holds done-callbacks without firing them - these tests exercise the cancel
    path, not result delivery."""

    def __init__(self, verdict: bool | None) -> None:
        self._verdict = verdict
        self.cancel_called = False

    def running(self) -> bool:
        return True

    def done(self) -> bool:
        return False

    def result(self, timeout: ty.Optional[float] = None) -> int:
        raise AssertionError("not expected in cancel-path tests")

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        return None

    def add_done_callback(self, fn: ty.Callable[["futures.PFuture[int]"], None]) -> None:
        pass

    def set_result(self, result: int) -> None:
        raise AssertionError("not expected in cancel-path tests")

    def set_exception(self, exception: BaseException) -> None:
        raise AssertionError("not expected in cancel-path tests")

    def cancel(self) -> bool | None:
        self.cancel_called = True
        return self._verdict


def test_try_cancel_on_non_cancellable_returns_none():
    class _NoCancel:
        def running(self) -> bool:
            return False

    assert futures.try_cancel(_NoCancel()) is None


def test_try_cancel_delegates_tristate():
    assert futures.try_cancel(_FakeCancellable(True)) is True
    assert futures.try_cancel(_FakeCancellable(False)) is False
    assert futures.try_cancel(_FakeCancellable(None)) is None


def test_resolved_future_cancel_is_none():
    # An already-resolved future has nothing to cancel.
    assert futures.try_cancel(futures.resolved(7)) is None


def test_lazy_future_cancel_delegates_to_realized_inner():
    fake = _FakeCancellable(True)
    lf = futures.LazyFuture(lambda: fake)
    assert lf.cancel() is True
    assert fake.cancel_called


def test_chained_future_cancel_true_settles_outer():
    # Inner reports it was cancelled -> outer must transition to a terminal
    # (cancelled) state so as_completed / .result() don't hang on a set_result
    # that will never arrive.
    inner = _FakeCancellable(True)
    outer = futures.chain_futures(inner, futures.identity)

    assert outer.cancel() is True
    assert inner.cancel_called
    assert outer.cancelled()
    assert outer.done()


def test_chained_future_cancel_false_when_inner_uncancellable():
    # Inner can't be cancelled (None) -> outer.cancel() reports False (stdlib
    # bool contract) and the outer is NOT forced terminal.
    inner = _FakeCancellable(None)
    outer = futures.chain_futures(inner, futures.identity)

    assert outer.cancel() is False
    assert not outer.cancelled()


def test_chained_future_cancel_with_real_inner_settles_terminal():
    # Cancelling a real concurrent.futures.Future inner cancels it; its
    # done-callback fires synchronously and settles the outer (done with
    # CancelledError). Either way the outer is terminal, so waiters don't hang.
    inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    outer = futures.chain_futures(inner, futures.identity)

    assert outer.cancel() is True
    assert outer.done()
    assert isinstance(outer.exception(timeout=1), concurrent.futures.CancelledError)


def test_translate_done_into_terminal_outer_is_noop():
    # The done-callback (translate_done) must be a no-op when the outer is
    # already terminal - e.g. a late inner completion racing a cancel - rather
    # than raising InvalidStateError by set_result-ing a finished future.
    outer: concurrent.futures.Future[int] = concurrent.futures.Future()
    outer.cancel()
    assert outer.cancelled()

    done_inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    done_inner.set_result(99)
    futures.translate_done(outer, futures.identity, done_inner)  # must not raise
    assert outer.cancelled()


def test_translate_done_does_not_swallow_translate_errors():
    # The InvalidStateError guard is scoped to the set_* write only - a raising
    # translate_result must still surface (as the outer's exception), not be
    # masked as if it were a cancel race.
    outer: concurrent.futures.Future[int] = concurrent.futures.Future()
    done_inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    done_inner.set_result(1)

    def boom(_: int) -> int:
        raise RuntimeError("translate blew up")

    futures.translate_done(outer, boom, done_inner)
    assert isinstance(outer.exception(timeout=1), RuntimeError)


def test_translate_done_propagates_inner_exception():
    # A raising inner result also surfaces on the outer (not swallowed).
    outer: concurrent.futures.Future[int] = concurrent.futures.Future()
    done_inner: concurrent.futures.Future[int] = concurrent.futures.Future()
    done_inner.set_exception(ValueError("inner failed"))

    futures.translate_done(outer, futures.identity, done_inner)
    assert isinstance(outer.exception(timeout=1), ValueError)
