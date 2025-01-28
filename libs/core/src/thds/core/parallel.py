"""Some utilities for running things in parallel - potentially large numbers of things.
"""

import concurrent.futures
import itertools
import traceback
import typing as ty
from collections import defaultdict
from dataclasses import dataclass
from uuid import uuid4

from thds.core import concurrency, config, files, log

PARALLEL_OFF = config.item("off", default=False, parse=config.tobool)
# if you want to simplify a stack trace, this may be your friend

R = ty.TypeVar("R")
T_co = ty.TypeVar("T_co", covariant=True)


logger = log.getLogger(__name__)


class IterableWithLen(ty.Protocol[T_co]):
    def __iter__(self) -> ty.Iterator[T_co]:
        ...  # pragma: no cover

    def __len__(self) -> int:
        ...  # pragma: no cover


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


def try_len(iterable: ty.Iterable[R]) -> ty.Optional[int]:
    try:
        return len(iterable)  # type: ignore
    except TypeError:
        return None


@dataclass
class Error:
    error: Exception


H = ty.TypeVar("H", bound=ty.Hashable)


def yield_all(
    thunks: ty.Iterable[ty.Tuple[H, ty.Callable[[], R]]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
) -> ty.Iterator[ty.Tuple[H, ty.Union[R, Error]]]:
    """Stream your results so that you don't have to load them all into memory at the same
    time (necessarily). Also, yield (rather than raise) Exceptions, wrapped as Errors.

    Additionally, if your iterable has a length and you do not provide
    a pre-sized Executor, we will create a ThreadPoolExecutor with the
    same size as your iterable. If you want to throttle the number of
    parallel tasks, you should provide your own Executor - and for
    most mops purposes it should be a ThreadPoolExecutor.
    """
    files.bump_limits()
    len_or_none = try_len(thunks)

    if PARALLEL_OFF() or len_or_none == 1:
        # don't actually transfer this to an executor we only have one task.
        for key, thunk in thunks:
            try:
                yield key, thunk()
            except Exception as e:
                yield key, Error(e)
        return  # we're done here

    executor_cm = executor_cm or concurrent.futures.ThreadPoolExecutor(
        max_workers=len_or_none or None, **concurrency.initcontext()
    )  # if len_or_none turns out to be zero, swap in a None which won't kill the executor
    with executor_cm as executor:
        keys_onto_futures = {key: executor.submit(thunk) for key, thunk in thunks}
        future_ids_onto_keys = {id(future): key for key, future in keys_onto_futures.items()}
        for future in concurrent.futures.as_completed(keys_onto_futures.values()):
            thunk_key = future_ids_onto_keys[id(future)]
            try:
                yield thunk_key, ty.cast(R, future.result())
            except Exception as e:
                yield thunk_key, Error(e)


def failfast(results: ty.Iterable[ty.Tuple[H, ty.Union[R, Error]]]) -> ty.Iterator[ty.Tuple[H, R]]:
    """Use in conjunction with `yield_all` to run things in parallel but to exit at the first sign
    of failure. More appropriate for small pipeline stages.
    """
    for key, res in results:
        if isinstance(res, Error):
            raise res.error
        yield key, res


def xf_mapping(thunks: ty.Mapping[H, ty.Callable[[], R]]) -> ty.Iterator[ty.Tuple[H, R]]:
    return failfast(yield_all(IteratorWithLen(len(thunks), thunks.items())))


def create_keys(iterable: ty.Iterable[R]) -> ty.Iterator[ty.Tuple[str, R]]:
    """Use this if you wanted to call yield_all with a list or other sequence that
    has no keys, and you don't need to 'track' the correspondence of input thunks to
    output results.
    """
    with_keys: ty.Iterable[ty.Tuple[str, R]] = ((uuid4().hex, item) for item in iterable)
    try:
        return IteratorWithLen(len(iterable), with_keys)  # type: ignore[arg-type]
    except TypeError:
        return iter(with_keys)


def yield_results(
    thunks: ty.Iterable[ty.Callable[[], R]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
    error_fmt: ty.Callable[[str], str] = lambda x: x,
    success_fmt: ty.Callable[[str], str] = lambda x: x,
    named: str = "",
    progress_logger: ty.Callable[[str], ty.Any] = logger.info,
) -> ty.Iterator[R]:
    """Yield only the successful results of your Callables/Thunks.

    If your iterable has a length, we will be able to log progress
    information. In most cases, this will be advantageous for you.

    Each task will fail or succeed separately without impacting other tasks.

    However, if any Exceptions are raised in any task, an Exception
    will be raised at the end of execution to indicate that not all
    tasks were successful. If you wish to capture Exceptions alongside
    results, use `yield_all` instead.
    """

    exceptions: ty.List[Exception] = list()

    num_tasks = try_len(thunks)
    num_tasks_log = "" if not num_tasks else f" of {num_tasks}"
    named = f" {named} " if named else " result "

    for i, (_key, res) in enumerate(
        yield_all(create_keys(thunks), executor_cm=executor_cm),
        start=1,
    ):
        if not isinstance(res, Error):
            errors = error_fmt(f"; {len(exceptions)} tasks have raised exceptions") if exceptions else ""
            progress_logger(success_fmt(f"Yielding{named}{i}{num_tasks_log} {errors}"))
            yield res
        else:
            exceptions.append(res.error)
            # print tracebacks as we go, so as not to defer potentially-helpful
            # debugging information while a long run is ongoing.
            traceback.print_exception(type(res.error), res.error, res.error.__traceback__)
            logger.error(  # should only use logger.exception from an except block
                error_fmt(
                    f"Task {i}{num_tasks_log} errored with {type(res.error).__name__}({res.error})"
                )
            )

    summarize_exceptions(error_fmt, exceptions)


def summarize_exceptions(
    error_fmt: ty.Callable[[str], str],
    exceptions: ty.List[Exception],
) -> None:
    if exceptions:
        # group by type
        by_type = defaultdict(list)
        for exc in exceptions:
            by_type[type(exc)].append(exc)

        # log the count for each Exception type
        most_common_type = None
        max_count = 0
        for _type, excs in by_type.items():
            logger.error(error_fmt(f"{len(excs)} tasks failed with exception: " + _type.__name__))
            if len(excs) > max_count:
                max_count = len(excs)
                most_common_type = _type

        logger.info("Raising one of the most common exception type.")
        raise by_type[most_common_type][0]  # type: ignore
