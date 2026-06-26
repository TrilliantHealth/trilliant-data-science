import concurrent.futures
import typing as ty
from dataclasses import dataclass
from functools import partial

from typing_extensions import ParamSpec

from . import lazy

R = ty.TypeVar("R")


@ty.runtime_checkable
class Cancellable(ty.Protocol):
    """A future-like that can try to cancel its underlying work.

    Deliberately NOT part of `PFuture` (which is the common subset of
    `concurrent.futures.Future` and omits cancellation). `cancel()` is tri-state:

    - `True`  - the underlying work was stopped.
    - `False` - supported, but couldn't (already finished/failed, or a cancel
      raced a completion).
    - `None`  - this future doesn't support cancellation at all (e.g. a future
      whose result already exists - nothing to cancel). Distinct from `False`:
      "no answer to give" vs. "tried and failed".

    The `None` case is why this is its own protocol rather than `bool`-only: a
    caller holding a concrete cancellable future can tell "not cancellable" from
    "cancel failed". At any boundary that demands the stdlib
    `Future.cancel() -> bool` contract, translate `None` to `False`."""

    def cancel(self) -> bool | None: ...


def try_cancel(future: ty.Any) -> bool | None:
    """Cancel `future` if it supports cancellation, else `None`. The single
    place that knows the tri-state convention, so delegating layers don't each
    re-implement the isinstance check."""
    if isinstance(future, Cancellable):
        return future.cancel()

    return None


class PFuture(ty.Protocol[R]):
    """
    A Protocol defining the behavior of a future-like object.

    This defines an interface for an object that acts as a placeholder
    for a result that will be available later. It is structurally
    compatible with concurrent.futures.Future but omits cancellation.
    """

    def running(self) -> bool:
        """Return True if the future is currently executing."""
        ...

    def done(self) -> bool:
        """Return True if the future is done (finished)."""
        ...

    def result(self, timeout: ty.Optional[float] = None) -> R:
        """Return the result of the work item.

        If the work item raised an exception, this method raises the same exception.
        If the timeout is reached, it raises TimeoutError.
        Other exceptions (e.g. CancelledException) may also be raised depending on the
        implementation.
        """
        ...

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        """
        Return the exception raised by the work item.

        Returns None if the work item completed without raising.
        If the timeout is reached, it raises TimeoutError.
        Other exceptions (e.g. CancelledException) may also be raised depending on the
        implementation.
        """
        ...

    def add_done_callback(self, fn: ty.Callable[["PFuture[R]"], None]) -> None:
        """
        Attaches a callable that will be called when the future is done.

        The callable will be called with the future object as its only
        argument.
        """
        ...

    def set_result(self, result: R) -> None:
        """Set the result of the future, marking it as done."""
        ...

    def set_exception(self, exception: BaseException) -> None:
        """Set the exception of the future, marking it as done."""
        ...


class LazyFuture(PFuture[R]):
    def __init__(self, mk_future: ty.Callable[[], PFuture[R]]) -> None:
        """mk_future should generally be serializable.

        It also needs to be repeatable - i.e. if it were resolved in two different
        processes, it should return the same 'result' (value or exception) in both.
        """
        self._mk_future = mk_future
        self._lazy_future = lazy.lazy(mk_future)

    def __getstate__(self) -> dict[str, ty.Any]:
        """Return the state of the LazyFuture for serialization."""
        return {"_mk_future": self._mk_future}

    def __setstate__(self, state: dict[str, ty.Any]) -> None:
        """Restore the state of the LazyFuture from serialization."""
        self._mk_future = state["_mk_future"]
        self._lazy_future = lazy.lazy(self._mk_future)

    def running(self) -> bool:
        """Return True if the future is currently executing."""
        return self._lazy_future().running()

    def cancel(self) -> bool | None:
        """Realize the inner future and delegate cancellation to it. Realizing
        is what produces the cancellable handle; the underlying work is expected
        to already exist by the time anyone cancels, so this reaches its handle
        rather than starting new work. `None` if the realized inner doesn't
        support cancellation."""
        return try_cancel(self._lazy_future())

    def done(self) -> bool:
        """Return True if the future is done (finished)."""
        return self._lazy_future().done()

    def result(self, timeout: ty.Optional[float] = None) -> R:
        """
        Return the result of the work item.

        If the work item raised an exception, this method raises the same
        exception. If the timeout is reached, it raises TimeoutError.
        """
        return self._lazy_future().result(timeout)

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        """
        Return the exception raised by the work item.

        Returns None if the work item completed without raising.
        If the timeout is reached, it raises TimeoutError.
        """
        return self._lazy_future().exception(timeout)

    def add_done_callback(self, fn: ty.Callable[["PFuture[R]"], None]) -> None:
        """
        Attaches a callable that will be called when the future is done.

        The callable will be called with the future object as its only
        argument.
        """
        self._lazy_future().add_done_callback(fn)

    def set_result(self, result: R) -> None:
        """Set the result of the future, marking it as done."""
        self._lazy_future().set_result(result)

    def set_exception(self, exception: BaseException) -> None:
        """Set the exception of the future, marking it as done."""
        self._lazy_future().set_exception(exception)


P = ParamSpec("P")


def make_lazy(mk_future: ty.Callable[P, PFuture[R]]) -> ty.Callable[P, LazyFuture[R]]:
    """Create a LazyFuture that will lazily resolve the given callable."""

    def mk_future_with_params(*args: P.args, **kwargs: P.kwargs) -> LazyFuture[R]:
        return LazyFuture(partial(mk_future, *args, **kwargs))

    return mk_future_with_params


@dataclass(frozen=True)
class ResolvedFuture(PFuture[R]):
    _result: R
    _done: bool = True

    def running(self) -> bool:
        return False

    def cancel(self) -> None:
        """Nothing to cancel: the result already exists."""
        return None

    def done(self) -> bool:
        return self._done

    def result(self, timeout: ty.Optional[float] = None) -> R:
        return self._result

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        return None

    def add_done_callback(self, fn: ty.Callable[["PFuture[R]"], None]) -> None:
        fn(self)

    def set_result(self, result: R) -> None:
        raise RuntimeError("Cannot set result on a resolved future.")

    def set_exception(self, exception: BaseException) -> None:
        raise RuntimeError("Cannot set exception on a resolved future.")


def resolved(result: R) -> ResolvedFuture[R]:
    return ResolvedFuture(result)


# what's below doesn't absolutely have to exist here, as it adds a 'dependency' on
# concurrent.futures... but practically speaking I think that's a pretty safe default
# 'actual Future' type.

R1 = ty.TypeVar("R1")


def identity(x: R) -> R:
    return x


def _settle(do_settle: ty.Callable[[], None]) -> None:
    """Run a terminal set_result/set_exception, tolerating the one case where it
    races a concurrent cancel of the target: `translate_done`'s done() guard
    passed, then `_ChainedFuture.cancel` settled the outer before this write. The
    outer is already terminal, so there is nothing to deliver - swallow exactly
    that InvalidStateError."""
    try:
        do_settle()
    except concurrent.futures.InvalidStateError:
        pass


def translate_done(
    out_future: PFuture[R1],
    translate_result: ty.Callable[[R], R1],
    done_fut: PFuture[R],
) -> None:
    # If the outer is already terminal - normally because it was cancelled (see
    # `_ChainedFuture.cancel`), which tore the work down - there is nothing to
    # deliver, and writing to it would raise InvalidStateError. A done-callback
    # that still fires (the inner completing concurrently with, or just after,
    # the cancel) skips the work here; the `_settle` calls below also tolerate
    # losing the race after this check.
    if out_future.done():
        return

    # Compute the outcome OUTSIDE the settle, so an InvalidStateError can only
    # come from the set_* call (the cancel race above, lost between the done()
    # check and the write here) - never masked from done_fut.result() or the
    # user's translate_result.
    try:
        translated = translate_result(done_fut.result())
    except Exception as e:
        # `as e` is cleared when the except block exits, so the lambda (run
        # later, inside _settle) would see an unbound name - bind it to a
        # surviving local first.
        exc = e
        _settle(lambda: out_future.set_exception(exc))
        return

    _settle(lambda: out_future.set_result(translated))


class _ChainedFuture(concurrent.futures.Future, ty.Generic[R1]):  # type: ignore[type-arg]
    """The outer future returned by `chain_futures`.

    It subclasses `concurrent.futures.Future` so it still satisfies the
    `isinstance` checks that `concurrent.futures.as_completed` (and our
    `reify_future`) rely on - that machinery reaches into `_state` /
    `_condition` / `_waiters` directly, so a duck-typed object would crash
    there. Result delivery still flows through the parent's `set_result` via
    the inner's done-callback (see `chain_futures`).

    What it adds over a bare `Future`: it retains its `inner_future` and
    delegates `running()` and `cancel()` to it. A bare chained outer is
    constructed un-started and only `set_result`-ed when the inner finishes, so
    its own `_state` is never RUNNING - `running()` would always be False even
    while the inner's work is mid-flight, and `cancel()` would only ever cancel
    the un-started sink, never the work. Delegating fixes both."""

    def __init__(self, inner_future: PFuture[ty.Any]) -> None:
        # inner yields the pre-translation type; the outer yields R1 after
        # `translate_future` runs in the done-callback - so from this class's
        # perspective the inner is untyped (the bridge is in `chain_futures`).
        super().__init__()
        self._inner_future = inner_future

    def running(self) -> bool:
        return self._inner_future.running()

    def cancel(self) -> bool:
        """Cancel the inner work and reconcile this outer's state.

        Returns strict `bool` (not the inner's tri-state) to honor the stdlib
        `Future.cancel()` contract that `as_completed`'s internals rely on:
        both `None` (inner not cancellable) and `False` (tried, couldn't)
        collapse to `False`.

        When the inner is actually cancelled, the done-callback that normally
        copies its result onto this outer (registered by `chain_futures`) will
        never fire with a real result - so we must move this outer to a terminal
        state, or `as_completed` (and any `.result()` waiter) would block forever
        on a `set_result` that never comes. `super().cancel()` (the parent
        `Future.cancel()`) transitions the still-PENDING outer to CANCELLED,
        which `as_completed` treats as done. If the inner's work races and that
        callback fires anyway, it finds this outer already terminal and no-ops
        rather than erroring (it guards on `done()`)."""
        if not try_cancel(self._inner_future):
            return False

        # Inner work is gone; settle the outer so waiters don't hang.
        super().cancel()
        return True


def chain_futures(
    inner_future: PFuture[R],
    translate_future: ty.Callable[[R], R1],
) -> "_ChainedFuture[R1]":
    """Chain two futures together with a translator in the middle.

    Returns a `_ChainedFuture` that retains `inner_future` so `running()`
    reports the inner work's state, not the (never-started) outer sink's."""
    outer_future: _ChainedFuture[R1] = _ChainedFuture(inner_future)
    inner_future.add_done_callback(partial(translate_done, outer_future, translate_future))
    return outer_future


def reify_future(future: PFuture[R]) -> concurrent.futures.Future[R]:
    """Reify a PFuture into a concurrent.futures.Future."""
    if isinstance(future, concurrent.futures.Future):
        return future
    return chain_futures(future, identity)


def as_completed(
    futures: ty.Iterable[PFuture[R]],
) -> ty.Iterator[concurrent.futures.Future[R]]:
    """Return an iterator that yields futures as they complete.

    We do need to actually create Future objects here, using the add_done_callback method
    so that the these things actually work with concurrent.futures.as_completed.
    """
    yield from concurrent.futures.as_completed(map(reify_future, futures))
