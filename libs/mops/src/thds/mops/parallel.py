import concurrent
import typing as ty

from thds.core import inspect, log, parallel
from thds.core.parallel import (  # noqa: F401; for backward-compatibility, since these came from here originally.
    IterableWithLen,
    IteratorWithLen,
)
from thds.core.thunks import (  # noqa: F401; for backward-compatibility, since these came from here originally.
    Thunk,
    thunking,
)
from thds.termtool.colorize import colorized

ERROR = colorized(fg="white", bg="red")
DONE = colorized(fg="white", bg="blue")
R = ty.TypeVar("R")


def parallel_yield_results(
    thunks: ty.Iterable[ty.Callable[[], R]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
    named: str = "",
) -> ty.Iterator[R]:
    yield from parallel.yield_results(
        thunks,
        executor_cm=executor_cm,
        error_fmt=ERROR,
        success_fmt=DONE,
        named=named,
        progress_logger=log.getLogger(inspect.caller_module_name(__name__) or __name__).info,
    )


yield_results = parallel_yield_results
