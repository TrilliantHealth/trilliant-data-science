"""As a general rule, you should prefer to run `use_runner` tasks
inside threads rather than separate subprocesses, since by definition
there will be a separate process somewhere actually executing your
job. You'll save on memory, and you'll more easily be able to transfer
context to the tasks.

These are some basic utilities to try to make this work as smoothly as possible.
"""
import concurrent.futures
import itertools
import typing as ty
from collections import defaultdict

from typing_extensions import ParamSpec

from thds.core import concurrency, log

from ._utils.colorize import colorized
from ._utils.file_limits import bump_limits

P = ParamSpec("P")
R = ty.TypeVar("R")
T = ty.TypeVar("T")
T_co = ty.TypeVar("T_co", covariant=True)


ERROR = colorized(fg="white", bg="red")
DONE = colorized(fg="white", bg="blue")

logger = log.getLogger(__name__)


class IterableWithLen(ty.Protocol[T_co]):
    def __iter__(self) -> ty.Iterator[T_co]:
        ...

    def __len__(self) -> int:
        ...


class IteratorWithLen(ty.Generic[R]):
    """Suitable for a case where you know how many elements you have
    and you want to be able to represent that somewhere else but you
    don't want to 'realize' all the elements upfront.
    """

    def __init__(self, length: int, iterable: ty.Iterable[R]):
        self._length = length
        self._iterator = iter(iterable)

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> ty.Iterator[R]:
        return self

    def __next__(self) -> R:
        return next(self._iterator)

    @staticmethod
    def chain(a: IterableWithLen[R], b: IterableWithLen[R]) -> "IteratorWithLen[R]":
        return IteratorWithLen(len(a) + len(b), itertools.chain(a, b))

    @staticmethod
    def from_iwl(iwl: IterableWithLen[R]) -> "IteratorWithLen[R]":
        return IteratorWithLen(len(iwl), iwl)


class YieldingMapWithLen(ty.Generic[R]):
    """Defer a computation to when the next item is accessed, but still provide a total length."""

    def __init__(self, f: ty.Callable[[T], R], base_coll: ty.Iterable[T]):
        self.f = f
        self.base_coll = list(base_coll)
        self.i = -1

    def __len__(self) -> int:
        return len(self.base_coll)

    def __iter__(self) -> ty.Iterator[R]:
        return self

    def __next__(self) -> R:
        self.i += 1
        if self.i == len(self.base_coll):
            raise StopIteration
        return self.f(self.base_coll[self.i])


class Thunk(ty.Generic[R]):
    """Result-typed callable with arguments partially applied beforehand.

    Unused here but provided for the caller's use if desired.
    """

    def __init__(self, func: ty.Callable[P, R], *args: P.args, **kwargs: P.kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __str__(self) -> str:
        return f"Thunk({self.func.__name__}, {self.args}, {self.kwargs})"

    def __call__(self) -> R:
        return ty.cast(R, self.func(*self.args, **self.kwargs))


def parallel_yield_results(
    thunks: ty.Iterable[ty.Callable[[], R]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
) -> ty.Iterator[R]:
    """Stream your results so that you don't have to load them all into memory at the same time (necessarily).

    If your iterable has a length, we will be able to log progress
    information. In most cases, this will be advantageous for you.

    Additionally, if your iterable has a length and you do not provide
    a pre-sized Executor, we will create a ThreadPoolExecutor with the
    same size as your iterable. If you want to throttle the number of
    parallel tasks, you should provide your own Executor - and for
    most mops purposes it should be a ThreadPoolExecutor.

    Each task will fail or succeed separately without impacting other tasks.

    However, if any Exceptions are raised in any task, an Exception
    will be raised at the end of execution to indicate that not all
    tasks were successful. If you wish to capture Exceptions alongside
    results, you should wrap your thunks to return a Union type.

    """
    bump_limits()

    try:
        num_tasks = len(thunks)  # type: ignore
        num_tasks_log = f" of {num_tasks}"
    except TypeError:
        num_tasks = None  # use system default
        num_tasks_log = ""

    executor_cm = executor_cm or concurrent.futures.ThreadPoolExecutor(
        max_workers=num_tasks, **concurrency.initcontext()
    )
    exceptions: ty.List[Exception] = list()
    with executor_cm as executor:
        futures = [executor.submit(thunk) for thunk in thunks]
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                errors = (
                    ERROR(f"; {len(exceptions)} tasks have raised exceptions") if len(exceptions) else ""
                )
                res = ty.cast(R, future.result())
                logger.info(DONE(f"Yielding result {i}{num_tasks_log}{errors}"))
                yield res
            except Exception as e:
                logger.exception(ERROR(f"Task {i}{num_tasks_log} errored with {str(e)}"))
                exceptions.append(e)

    _summarize_exceptions(exceptions)


def _summarize_exceptions(exceptions: ty.List[Exception]):
    if not exceptions:
        return

    by_type = defaultdict(list)
    for exc in exceptions:
        by_type[type(exc)].append(exc)
        logger.error(ERROR("EXCEPTION"), exc_info=(type(exc), exc, exc.__traceback__))

    most_common_type = None
    max_count = 0
    for _type, excs in by_type.items():
        logger.error(ERROR(f"{len(excs)} tasks failed with exception: " + str(_type)))
        if len(excs) > max_count:
            max_count = len(excs)
            most_common_type = _type

    logger.info("Raising one of the most common exception type.")
    raise by_type[most_common_type][0]  # type: ignore


def thunking(func: ty.Callable[P, R]) -> ty.Callable[P, Thunk[R]]:
    """Converts a standard function into a function that accepts the
    exact same arguments but returns a Thunk - something ready to be
    executed but the execution itself is deferred.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Thunk[R]:
        return Thunk(func, *args, **kwargs)

    return wrapper
