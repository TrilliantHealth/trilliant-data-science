"""As a general rule, you should prefer to run `pure_remote` tasks
inside threads rather than separate subprocesses, since by definition
there will be a separate process somewhere actually executing your
job. You'll save on memory, and you'll more easily be able to transfer
context to the tasks.

These are some basic utilities to try to make this work as smoothly as possible.
"""
import concurrent.futures
import typing as ty
from collections import defaultdict

from typing_extensions import ParamSpec

from thds.core.log import getLogger

from ..colorize import colorized
from ._file_limits import bump_limits
from ._root import get_pipeline_id, set_pipeline_id
from .types import IterableWithLen

P = ParamSpec("P")
R = ty.TypeVar("R")
T = ty.TypeVar("T")


ERROR = colorized(fg="white", bg="red")
DONE = colorized(fg="white", bg="blue")

logger = getLogger(__name__)


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
        self.pipeline_id = get_pipeline_id()

    def __call__(self) -> R:
        set_pipeline_id(self.pipeline_id)
        return ty.cast(R, self.func(*self.args, **self.kwargs))


def parallel_yield_results(
    thunks: IterableWithLen[ty.Callable[[], R]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
) -> ty.Iterator[R]:
    """Stream your results so that you don't have to load them all into memory at the same time (necessarily).

    Somewhat robust to failures, in that it will allow each task to
    fail separately without causing all to fail.  Will log all
    exceptions and raise a final exception at the end if any are
    encountered.
    """
    _ = get_pipeline_id()  # force existence of lazily-generated pipeline id.
    bump_limits()
    num_tasks = len(thunks)
    executor_cm = executor_cm or concurrent.futures.ThreadPoolExecutor(max_workers=num_tasks)
    exceptions: ty.List[Exception] = list()
    with executor_cm as executor:
        futures = [executor.submit(thunk) for thunk in thunks]
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                errors = (
                    ERROR(f"; {len(exceptions)} tasks have raised exceptions") if len(exceptions) else ""
                )
                res = ty.cast(R, future.result())
                logger.info(DONE(f"Yielding result {i} of {num_tasks}{errors}"))
                yield res
            except Exception as e:
                logger.exception(ERROR(f"Task {i} of {num_tasks} errored with {str(e)}"))
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
