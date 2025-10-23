"""Some utilities for running things in parallel - potentially large numbers of things."""

import concurrent.futures
import itertools
import traceback
import typing as ty
from collections import defaultdict
from dataclasses import dataclass
from uuid import uuid4

from thds.core import concurrency, config, files, inspect, log

PARALLEL_OFF = config.item("off", default=False, parse=config.tobool)
# if you want to simplify a stack trace, this may be your friend

R = ty.TypeVar("R")
T_co = ty.TypeVar("T_co", covariant=True)


class IterableWithLen(ty.Protocol[T_co]):
    def __iter__(self) -> ty.Iterator[T_co]: ...  # pragma: no cover

    def __len__(self) -> int: ...  # pragma: no cover


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


def _get_caller_logger(named: str) -> ty.Callable[[str], ty.Any]:
    module_name = inspect.caller_module_name(__name__)
    if module_name:
        return log.getLogger(module_name).info if named else log.getLogger(module_name).debug
    return log.getLogger(__name__).debug  # if not named, we default to debug level


def yield_all(
    thunks: ty.Iterable[ty.Tuple[H, ty.Callable[[], R]]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
    fmt: ty.Callable[[str], str] = lambda x: x,
    error_fmt: ty.Callable[[str], str] = lambda x: x,
    named: str = "",
    progress_logger: ty.Optional[ty.Callable[[str], ty.Any]] = None,
) -> ty.Iterator[ty.Tuple[H, ty.Union[R, Error]]]:
    """Stream your results so that you don't have to load them all into memory at the same
    time (necessarily). Also, yield (rather than raise) Exceptions, wrapped as Errors.

    Additionally, if your iterable has a length and you do not provide
    a pre-sized Executor, we will create a ThreadPoolExecutor with the
    same size as your iterable. If you want to throttle the number of
    parallel tasks, you should provide your own Executor - and for
    most mops purposes it should be a ThreadPoolExecutor.

    Currently this function does not yield any results until the input iterable is exhausted,
    even though some of the thunks may have been submitted and the workers have returned a result
    to the local process.
    """
    files.bump_limits()
    len_or_none = try_len(thunks)

    num_tasks_log = "" if not len_or_none else f" of {len_or_none}"

    if PARALLEL_OFF() or (len_or_none == 1 and not executor_cm):
        # don't actually transfer this to an executor we only have one task.
        for key, thunk in thunks:
            try:
                yield key, thunk()
            except Exception as e:
                yield key, Error(e)
        return  # we're done here

    progress_logger = progress_logger or _get_caller_logger(named)

    executor_cm = executor_cm or concurrent.futures.ThreadPoolExecutor(
        max_workers=len_or_none or None, **concurrency.initcontext()
    )  # if len_or_none turns out to be zero, swap in a None which won't kill the executor
    with executor_cm as executor:
        keys_onto_futures = {key: executor.submit(thunk) for key, thunk in thunks}
        future_ids_onto_keys = {id(future): key for key, future in keys_onto_futures.items()}
        # While concurrent.futures.as_completed accepts an iterable as input, it
        # does not yield any completed futures until the input iterable is
        # exhausted.
        num_exceptions = 0
        for i, future in enumerate(concurrent.futures.as_completed(keys_onto_futures.values()), start=1):
            thunk_key = future_ids_onto_keys[id(future)]
            error_suffix = (
                error_fmt(f"; {num_exceptions} tasks have raised exceptions") if num_exceptions else ""
            )
            try:
                result = future.result()
                yielder: tuple[H, ty.Union[R, Error]] = thunk_key, ty.cast(R, result)
                name = named or result.__class__.__name__
            except Exception as e:
                yielder = thunk_key, Error(e)
                name = named or e.__class__.__name__
            finally:
                progress_logger(fmt(f"Yielding {name} {i}{num_tasks_log}") + error_suffix)
                yield yielder


def failfast(results: ty.Iterable[ty.Tuple[H, ty.Union[R, Error]]]) -> ty.Iterator[ty.Tuple[H, R]]:
    """Use in conjunction with `yield_all` to run things in parallel but to exit at the first sign
    of failure. More appropriate for small pipeline stages.
    """
    for key, res in results:
        if isinstance(res, Error):
            raise res.error
        yield key, res


def xf_mapping(
    thunks: ty.Mapping[H, ty.Callable[[], R]], named: str = ""
) -> ty.Iterator[ty.Tuple[H, R]]:
    return failfast(yield_all(IteratorWithLen(len(thunks), thunks.items()), named=named))


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


ERROR_LOGGER = log.getLogger(__name__)


def yield_results(
    thunks: ty.Iterable[ty.Callable[[], R]],
    *,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
    error_fmt: ty.Callable[[str], str] = lambda x: x,
    success_fmt: ty.Callable[[str], str] = lambda x: x,
    named: str = "",
    progress_logger: ty.Optional[ty.Callable[[str], ty.Any]] = None,
) -> ty.Iterator[R]:
    """Yield only the successful results of your Callables/Thunks. Continue despite errors.

    If your iterable has a length, we will be able to log progress
    information. In most cases, this will be advantageous for you.

    Each task will fail or succeed separately without impacting other tasks.

    However, if any Exceptions are raised in any task, an Exception
    will be raised at the end of execution to indicate that not all
    tasks were successful. If you wish to capture Exceptions alongside
    results, use `yield_all` instead.
    """

    exceptions: ty.List[Exception] = list()

    for i, (_key, res) in enumerate(
        yield_all(
            create_keys(thunks),
            executor_cm=executor_cm,
            named=named,
            progress_logger=progress_logger,
            fmt=success_fmt,
            error_fmt=error_fmt,
        ),
        start=1,
    ):
        if not isinstance(res, Error):
            yield res
        else:
            exceptions.append(res.error)
            # print tracebacks as we go, so as not to defer potentially-helpful
            # debugging information while a long run is ongoing.
            traceback.print_exception(type(res.error), res.error, res.error.__traceback__)
            ERROR_LOGGER.error(  # should only use logger.exception from an except block
                error_fmt(f"Task {i} errored with {type(res.error).__name__}({res.error})")
            )

    summarize_exceptions(error_fmt, exceptions)
    # TODO - when `core` moves to 3.11 start using an ExceptionGroup here


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
            ERROR_LOGGER.error(error_fmt(f"{len(excs)} tasks failed with exception: " + _type.__name__))
            if len(excs) > max_count:
                max_count = len(excs)
                most_common_type = _type

        ERROR_LOGGER.info("Raising one of the most common exception type.")
        raise by_type[most_common_type][0]  # type: ignore
