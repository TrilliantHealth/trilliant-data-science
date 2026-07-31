"""Shared waiter for lease-blocked invocations.

When submit() finds another process holding an invocation's lease, it registers the wait
here and returns a pending future immediately, instead of parking its thread in a
sleep/check loop. A single daemon thread polls every awaited invocation with exponential
backoff; each poll either resolves the future with the other caller's now-existing
result, takes over an expired lease (dispatching our own invocation on a fresh thread),
or reschedules itself. When the lease holder is a thread in this same process, the
waiter also subscribes to the holder's completion (see same_process_in_flight.py) and wakes the
moment it settles, instead of waiting out the timed backoff.

No future may ever be stranded: every failure path, including errors inside the poll
itself, settles the future with an exception.
"""

import concurrent.futures
import heapq
import itertools
import threading
import time
import typing as ty
from dataclasses import dataclass

from thds.core import cache, futures, log
from thds.termtool.colorize import colorized, make_colorized_out

from .._futures import MopsFuture
from ..core import lease, metadata
from . import same_process_in_flight

logger = log.getLogger(__name__)
_LogAwaited = make_colorized_out(colorized(fg="white", bg="#800080"), out=logger.info, fmt_str=" {} ")

# The backoff starts fast enough that a result produced in seconds is picked up in
# seconds (1+2+4+8: a <10s result is seen by ~15s), and is capped so long waits poll
# storage no more often than every _MAX_BACKOFF_S.
_INITIAL_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 22.0
_LOG_INTERVAL_S = 6 * _MAX_BACKOFF_S  # ~2 minutes between repeat 'still waiting' logs

R = ty.TypeVar("R")
ValueAndMetadata = tuple[R, metadata.ResultMetadata | None]


def _resolved_tuple_future(
    value_and_metadata: "ValueAndMetadata[R]",
) -> "concurrent.futures.Future[ValueAndMetadata[R]]":
    fut: "concurrent.futures.Future[ValueAndMetadata[R]]" = concurrent.futures.Future()
    fut.set_result(value_and_metadata)
    return fut


def _unpickled(future: "futures.PFuture[ValueAndMetadata[R]]") -> "futures.PFuture[ValueAndMetadata[R]]":
    return future


class _AwaitedFuture(concurrent.futures.Future, ty.Generic[R]):  # type: ignore[type-arg]
    """Yields (value, result_metadata) for a lease-blocked invocation.

    MopsFutures cross process boundaries via pickle, and the waiter daemon that would
    resolve this future dies with the process that created it - so a pending instance
    cannot cross as-is. Pickling one:

    - after a lease takeover, delegates to our own invocation's future, which is
      picklable and lazily resumable in the receiving process - the same object a
      first-try lease acquisition would have handed out;
    - while still waiting on another process's live lease, blocks until resolution,
      reproducing the pre-3.25 behavior where a lease-blocked submit() did not return
      until the wait was over. If the awaited invocation failed, pickling raises its
      exception.
    """

    def __init__(self, memo_uri: str) -> None:
        super().__init__()
        self.memo_uri = memo_uri
        self._takeover_future: MopsFuture[R] | None = None

    def __reduce__(self) -> tuple:
        takeover_future = self._takeover_future
        if takeover_future is not None and not self.done():
            return _unpickled, (takeover_future.tuple_future(),)

        if not self.done():
            logger.info(
                "Pickling a pending awaited-lease future for %s - blocking until it resolves.",
                self.memo_uri,
            )
        return _resolved_tuple_future, (self.result(),)


@dataclass
class _AwaitedLease(ty.Generic[R]):
    memo_uri: str
    what: str  # 'value' or 'result', matching the runner's log phrasing
    outer: "_AwaitedFuture[R]"
    check_result: ty.Callable[[], ty.Any | None]
    unwrap: ty.Callable[[ty.Any], "ValueAndMetadata[R]"]  # raises memoized exceptions
    acquire_lease: ty.Callable[[], lease.LeaseAcquired | None]
    invoke_with_lease: ty.Callable[[lease.LeaseAcquired], MopsFuture[R]]
    backoff_s: float
    waiting_since: float
    next_log_at: float
    subscribed_holder: same_process_in_flight.Completes | None = None


def _settle(do_settle: ty.Callable[[], None]) -> None:
    # tolerate exactly the race where the caller cancelled the future between our
    # done() check and this write - a terminal future has nothing to deliver.
    try:
        do_settle()
    except concurrent.futures.InvalidStateError:
        pass


def _bridge(outer: "concurrent.futures.Future[ValueAndMetadata[R]]", mops_future: MopsFuture[R]) -> None:
    """Copy a finished MopsFuture's outcome onto the outer tuple-future."""
    try:
        value = mops_future.result()
    except BaseException as e:  # noqa: B036 - includes CancelledError; delivered to waiters
        exc = e
        _settle(lambda: outer.set_exception(exc))
        return

    _settle(lambda: outer.set_result((value, mops_future.result_metadata)))


def _take_over(item: _AwaitedLease, lease_owned: lease.LeaseAcquired) -> None:
    """Runs on its own (non-daemon) thread: with a synchronous shim, dispatch blocks for
    the entire computation, and we must neither stall the waiter daemon nor let process
    exit kill the work mid-flight."""
    try:
        mops_future = item.invoke_with_lease(lease_owned)
    except Exception as e:
        exc = e
        logger.exception("Takeover invocation for %s failed.", item.memo_uri)
        _settle(lambda: item.outer.set_exception(exc))
        return

    item.outer._takeover_future = mops_future  # from here on, pickling delegates instead of blocking
    mops_future.add_done_callback(lambda _f: _bridge(item.outer, mops_future))


_HEAP: list[tuple[float, int, _AwaitedLease]] = []
_HEAP_LOCK = threading.Lock()
_ITEM_ADDED = threading.Event()
_HEAP_TIEBREAKER = itertools.count()  # _AwaitedLease itself is not orderable


def _schedule(item: _AwaitedLease, delay_s: float) -> None:
    with _HEAP_LOCK:
        heapq.heappush(_HEAP, (time.monotonic() + delay_s, next(_HEAP_TIEBREAKER), item))
    _ITEM_ADDED.set()


def _subscribe_in_process(item: _AwaitedLease) -> None:
    item.subscribed_holder = same_process_in_flight.subscribe(
        item.memo_uri, item.subscribed_holder, lambda: _schedule(item, 0.0)
    )


def _poll(item: _AwaitedLease) -> float | None:
    """One check/acquire attempt. Returns seconds until the next poll, or None if this
    invocation no longer needs polling (settled, taken over, or cancelled)."""
    if item.outer.done():  # cancelled by the caller - stop polling.
        return None

    result = item.check_result()
    if result is not None:
        _LogAwaited(
            f"{item.what} for {item.memo_uri} was found after waiting"
            f" {time.monotonic() - item.waiting_since:.0f}s for the lease."
        )
        try:
            value_and_md = item.unwrap(result)
        except Exception as e:  # includes the memoized exception, if the result was an Error
            exc = e
            _settle(lambda: item.outer.set_exception(exc))
            return None

        _settle(lambda: item.outer.set_result(value_and_md))
        return None

    lease_owned = item.acquire_lease()
    if lease_owned is not None:
        logger.info(f"Took over expired lease for {item.memo_uri} - invoking ourselves.")
        threading.Thread(target=_take_over, args=(item, lease_owned), name="mops-lease-takeover").start()
        return None

    _subscribe_in_process(item)
    if time.monotonic() >= item.next_log_at:
        _LogAwaited(
            f"{item.what} for {item.memo_uri} does not exist, but the lease is held by"
            f" another caller. Still waiting after"
            f" {time.monotonic() - item.waiting_since:.0f}s."
        )
        item.next_log_at = time.monotonic() + _LOG_INTERVAL_S
    item.backoff_s = min(item.backoff_s * 2, _MAX_BACKOFF_S)
    return item.backoff_s


def _poll_and_reschedule(item: _AwaitedLease) -> None:
    try:
        delay_s = _poll(item)
    except Exception as e:
        # e.g. a network failure during check/acquire. Fail the future, exactly as the
        # pre-3.25 blocking wait raised out of submit().
        exc = e
        logger.exception(
            "Error while awaiting the lease for %s; failing the invocation future.", item.memo_uri
        )
        _settle(lambda: item.outer.set_exception(exc))
        return

    if delay_s is not None:
        _schedule(item, delay_s)


def _waiter_daemon() -> None:
    while True:
        with _HEAP_LOCK:
            next_wakeup = _HEAP[0][0] if _HEAP else None

        if next_wakeup is None:
            _ITEM_ADDED.wait()
            _ITEM_ADDED.clear()
            continue

        if _ITEM_ADDED.wait(timeout=max(0.0, next_wakeup - time.monotonic())):
            _ITEM_ADDED.clear()  # a new item may be due sooner - re-evaluate.
            continue

        while True:
            with _HEAP_LOCK:
                if not _HEAP or _HEAP[0][0] > time.monotonic():
                    break

                _, _, item = heapq.heappop(_HEAP)
            _poll_and_reschedule(item)


@cache.locking
def _ensure_daemon() -> None:
    threading.Thread(target=_waiter_daemon, daemon=True, name="mops-lease-waiter").start()


def future_awaiting_lease(
    memo_uri: str,
    *,
    what: str,
    check_result: ty.Callable[[], ty.Any | None],
    unwrap: ty.Callable[[ty.Any], "ValueAndMetadata[R]"],
    acquire_lease: ty.Callable[[], lease.LeaseAcquired | None],
    invoke_with_lease: ty.Callable[[lease.LeaseAcquired], MopsFuture[R]],
) -> "concurrent.futures.Future[ValueAndMetadata[R]]":
    """Register a lease-blocked invocation with the shared waiter and return a pending
    future of (value, result_metadata). Never blocks.

    The future resolves with another process's result, with our own invocation's result
    after taking over an expired lease, or with an exception - either a memoized one or
    any error encountered while waiting. Cancelling it stops the polling; it does not
    (and could not) cancel the other process's work.
    """
    now = time.monotonic()
    item = _AwaitedLease(
        memo_uri=memo_uri,
        what=what,
        outer=_AwaitedFuture(memo_uri),
        check_result=check_result,
        unwrap=unwrap,
        acquire_lease=acquire_lease,
        invoke_with_lease=invoke_with_lease,
        backoff_s=_INITIAL_BACKOFF_S,
        waiting_since=now,
        next_log_at=now + _LOG_INTERVAL_S,
    )
    _LogAwaited(
        f"{what} for {memo_uri} does not exist, but the lease is held by another caller."
        " Waiting for it in the background."
    )
    _ensure_daemon()
    _subscribe_in_process(item)
    _schedule(item, item.backoff_s)
    return item.outer
