import concurrent.futures
import typing as ty
from dataclasses import dataclass
from functools import partial

from typing_extensions import ParamSpec

from . import lazy

R = ty.TypeVar("R")


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

    def done(self) -> bool:
        return self._done

    def result(self, timeout: ty.Optional[float] = None) -> R:
        return self._result

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        return None

    def add_done_callback(self, fn: ty.Callable[["PFuture[R]"], None]) -> None:
        fn(self)


def resolved(result: R) -> ResolvedFuture[R]:
    return ResolvedFuture(result)


# what's below doesn't absolutely have to exist here, as it adds a 'dependency' on
# concurrent.futures... but practically speaking I think that's a pretty safe default
# 'actual Future' type.

R1 = ty.TypeVar("R1")


def identity(x: R) -> R:
    return x


def translate_done(
    cfuture: concurrent.futures.Future[R1],
    translate_result: ty.Callable[[R], R1],
    done_fut: PFuture[R],
) -> None:
    try:
        result = done_fut.result()
        cfuture.set_result(translate_result(result))
    except Exception as e:
        cfuture.set_exception(e)


def _translate_future(
    future: PFuture[R], translate_future: ty.Callable[[R], R1]
) -> concurrent.futures.Future[R1]:
    """Convert a PFuture to a concurrent.futures.Future."""
    out_future = concurrent.futures.Future[R1]()
    done_cb = partial(translate_done, out_future, translate_future)
    future.add_done_callback(done_cb)
    return out_future


def reify_future(future: PFuture[R]) -> concurrent.futures.Future[R]:
    """Reify a PFuture into a concurrent.futures.Future."""
    if isinstance(future, concurrent.futures.Future):
        return future
    return _translate_future(future, identity)


@dataclass(frozen=True)
class _SerializableFutureChainMaker(ty.Generic[R, R1]):
    """
    A Future that chains the result of one future to another.

    Used to transform a future of one type into another.

    The two fields must be serializable.
    """

    _inner_future: PFuture[R]
    _translate_result: ty.Callable[[R], R1]

    def __call__(self) -> concurrent.futures.Future[R1]:
        return _translate_future(self._inner_future, self._translate_result)


def chain_lazy_future(
    translate_result: ty.Callable[[R], R1], inner_future: PFuture[R]
) -> LazyFuture[R1]:
    """This mostly only makes sense to use if you have a Lazy inner Future that you want to
    'transform' _lazily_ into a different type of Future.
    """
    return LazyFuture(_SerializableFutureChainMaker(inner_future, translate_result))


def as_completed(
    futures: ty.Iterable[PFuture[R]],
) -> ty.Iterator[concurrent.futures.Future[R]]:
    """Return an iterator that yields futures as they complete.

    We do need to actually create Future objects here, using the add_done_callback method
    so that the these things actually work with concurrent.futures.as_completed.
    """
    yield from concurrent.futures.as_completed(map(reify_future, futures))
