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


def translate_done(
    out_future: PFuture[R1],
    translate_result: ty.Callable[[R], R1],
    done_fut: PFuture[R],
) -> None:
    try:
        result = done_fut.result()
        out_future.set_result(translate_result(result))
    except Exception as e:
        out_future.set_exception(e)


OF_R1 = ty.TypeVar("OF_R1", bound=PFuture)


def chain_futures(
    inner_future: PFuture[R],
    outer_future: OF_R1,
    translate_future: ty.Callable[[R], R1],
) -> OF_R1:
    """Chain two futures together with a translator in the middle."""
    outer_done_cb = partial(translate_done, outer_future, translate_future)
    inner_future.add_done_callback(outer_done_cb)
    return outer_future


def reify_future(future: PFuture[R]) -> concurrent.futures.Future[R]:
    """Reify a PFuture into a concurrent.futures.Future."""
    if isinstance(future, concurrent.futures.Future):
        return future
    return chain_futures(future, concurrent.futures.Future[R](), identity)


def as_completed(
    futures: ty.Iterable[PFuture[R]],
) -> ty.Iterator[concurrent.futures.Future[R]]:
    """Return an iterator that yields futures as they complete.

    We do need to actually create Future objects here, using the add_done_callback method
    so that the these things actually work with concurrent.futures.as_completed.
    """
    yield from concurrent.futures.as_completed(map(reify_future, futures))
