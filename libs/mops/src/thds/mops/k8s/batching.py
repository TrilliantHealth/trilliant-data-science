"""The basic idea of this module is that different threads can submit _parts_ of a job to a batcher,
and immediately get the job name back, while the batcher itself defers creating the job until the
batch is full, or when the process exits.

The theory is that will get used in processes whose only responsibility is to create jobs,
so waiting on atexit to create the final batch is not an issue.

If you want a batcher that has a more context-manager-like behavior, you can write one of
those, but it wouldn't work well with a concurrent.futures Executor-style approach, since
those don't have an explicit shutdown procedure that we can hook to call __exit__.
"""

import atexit
import concurrent.futures
import itertools
import multiprocessing
import os
import threading
import typing as ty
from functools import wraps

from thds.core import cpus, futures, log

from . import _launch, auth, counts

T = ty.TypeVar("T")
logger = log.getLogger(__name__)

F = ty.TypeVar("F", bound=ty.Callable)


def _clear_config_cache(f: F) -> F:
    @wraps(f)
    def _wrapped(*args, **kwargs):  # type: ignore
        auth.load_config.cache_clear()  # type: ignore
        return f(*args, **kwargs)

    return ty.cast(F, _wrapped)


class _AtExitBatcher(ty.Generic[T]):
    def __init__(self, batch_processor: ty.Callable[[ty.Collection[T]], None]) -> None:
        self.batch: list[T] = []
        self._registered = False
        self._lock = threading.RLock()
        self._batch_processor = batch_processor

    def add(self, item: T) -> None:
        with self._lock:
            if not self._registered:
                # The kubernetes python SDK _also_ has an atexit handler, which
                # removes the temp file containing the SSL cert. That atexit
                # handler is called before this handler, since it is registered
                # after. If we reuse the cached config, which references that
                # temp file which no longer exists, we get the error:
                # `SSLError(FileNotFoundError(2, 'No such file or directory'))`.
                # Instead, we first clear the cached config, which causes the
                # cert file to be recreated when the config is loaded again.
                atexit.register(_clear_config_cache(self.process))
                # ensure we flush on process exit, since we don't know how many items are coming
                self._registered = True
            self.batch.append(item)

    def process(self) -> None:
        if self.batch:
            with self._lock:
                if self.batch:
                    self._batch_processor(self.batch)
                    self.batch = []


class K8sJobBatchingShim(_AtExitBatcher[str]):
    """Thread-safe for use within a single process by multiple threads."""

    def __init__(
        self,
        submit_func: ty.Callable[[ty.Collection[str]], ty.Any],
        max_batch_size: int,
        job_counter: counts.MpValue[int],
        job_prefix: str = "",
    ) -> None:
        """submit_func in particular should be a closure around whatever setup you need to
        do to call back into a function that is locally wrapped with a k8s shim that will
        ultimately call k8s.launch. Notably, you
        """
        super().__init__(self._process_batch)
        self._max_batch_size = max_batch_size
        self._job_counter = job_counter
        self._job_name = ""
        self._job_prefix = job_prefix
        self._submit_func = submit_func

    def _get_new_name(self) -> str:
        # counts.inc takes a multiprocess lock. do not forget this!
        job_num = counts.inc(self._job_counter)
        return _launch.construct_job_name(self._job_prefix, counts.to_name(job_num))

    def add_to_named_job(self, args_builder: ty.Callable[[], ty.Sequence[str]]) -> str:
        """Returns job name for the invocation. args_builder should be a closure
        around any additional state needed for running a batch. We do it this
        way so that this function can be called within the same lock.
        """
        with self._lock:
            if not self._job_name:
                self._job_name = self._get_new_name()
            if len(self.batch) >= self._max_batch_size:
                self.process()
                self._job_name = self._get_new_name()

            super().add(" ".join(args_builder()))
            return self._job_name

    def _process_batch(self, batch: ty.Collection[str]) -> None:
        with _launch.JOB_NAME.set(self._job_name):
            log_lvl = logger.warning if len(batch) < self._max_batch_size else logger.info
            log_lvl(f"Processing batch of len {len(batch)} with job name {self._job_name}")
            self._submit_func(batch)


_BATCHER: ty.Optional[K8sJobBatchingShim] = None


def init_batcher(
    submit_func: ty.Callable[[ty.Collection[str]], ty.Any],
    func_max_batch_size: int,
    job_counter: counts.MpValue[int],
    job_prefix: str = "",
) -> None:
    # for use with multiprocessing pool initializer
    global _BATCHER
    if _BATCHER is not None:
        logger.warning("Batcher is already initialized; reinitializing will reset the job name.")
        return

    _BATCHER = K8sJobBatchingShim(submit_func, func_max_batch_size, job_counter, job_prefix)


def init_batcher_with_unpicklable_submit_func(
    make_submit_func: ty.Callable[[T], ty.Callable[[ty.Collection[str]], ty.Any]],
    submit_func_arg: T,
    func_max_batch_size: int,
    job_counter: counts.MpValue[int],
    job_prefix: str = "",
) -> None:
    """Use this if you want to have an unpicklable submit function - because applying make_submit_func(submit_func_arg)
    will happen inside the pool worker process after all the pickling/unpickling has happened.
    """
    return init_batcher(
        make_submit_func(submit_func_arg), func_max_batch_size, job_counter, job_prefix=job_prefix
    )


def make_counting_process_pool_executor(
    make_submit_func: ty.Callable[[T], ty.Callable[[ty.Collection[str]], ty.Any]],
    submit_func_arg: T,
    max_batch_size: int,
    name_prefix: str = "",
    max_workers: int = 0,
) -> concurrent.futures.ProcessPoolExecutor:
    """Creates a ProcessPoolExecutor that uses the batching shim for job submission.

    We are introducing this because we see segfaults prior to Python 3.12 related to this issue:
    https://github.com/python/cpython/issues/77377

    And it would seem that this had to do with creating mp.Values using a 'fork' start
    method, and then passing those to a ProcessPoolExecutor with
    mp_context=multiprolcessing.get_context('spawn'). So we can help you avoid that by creating
    the mp.Value for you, alongside its ProcessPoolExecutor.

    NOTE!!

    You should only have one of these per process at a time, because we're doing spooky
    things with the Job Counter.  In fact, you should probably only create one of these
    _ever_ within a single logical 'application'.

    If you fail to heed this advice, you will get weird launched/finished counts at a
    minimum. Although these job counts are not mission-critical, you _will_ be confused.
    """
    start_method: str = "spawn"
    # 'spawn' prevents weird batch processing deadlocks that seem to only happen on Linux with 'fork'.
    # it is strongly recommended to use 'spawn' for this reason.

    mp_context = multiprocessing.get_context(start_method)
    launch_count = mp_context.Value("i", 0)
    # even though i want to assign this to a global, I also want to prevent
    # any possible race condition where i somehow use a different thread's LAUNCH_COUNT
    # when i create the ProcessPoolExecutor a few lines below.
    counts.LAUNCH_COUNT = launch_count
    counts.FINISH_COUNT = mp_context.Value("i", 0)  # we don't use this here; we just reset it to zero.
    # SPOOKY - reset the global finish counter and make it be the same 'type'
    return concurrent.futures.ProcessPoolExecutor(
        max_workers=max_workers or cpus.available_cpu_count(),
        initializer=init_batcher_with_unpicklable_submit_func,
        initargs=(
            make_submit_func,
            submit_func_arg,
            max_batch_size,
            launch_count,
            "-".join([name_prefix, str(os.getpid())]),
        ),
        mp_context=mp_context,
    )


def add_to_batch(args_builder: ty.Callable[[], ty.Sequence[str]]) -> futures.PFuture[bool]:
    """
    args_builder should be a closure around any additional state that needs to
    be kept for batching, e.g. it can be used for determining batch cpu count
    from individual invocation args.
    """
    # This thing needs to return a lazy Uncertain Future that contains a job name, so that Job can be polled on
    # ... but the job does not exist yet! So the batcher is in charge of creating the job name
    # upfront, and then ensuring that it gets used when the job is created.
    assert _BATCHER is not None, "Batcher must be initialized before using the batching shim."
    job_name = _BATCHER.add_to_named_job(args_builder)
    return _launch.create_lazy_job_logging_future(job_name)


def shim(args: ty.Sequence[str]) -> futures.PFuture[bool]:
    """Use this shim if you don't need any additional setup around running a batch"""
    return add_to_batch(lambda: args)


def batched(iterable: ty.Iterable[T], n: int, *, strict: bool = False) -> ty.Iterator[tuple[T, ...]]:
    """Just a utility for pre-batching if you're using multiprocessing to create batches."""
    # TODO get rid of this when we go to Python 3.12+ which has itertools.batched
    #
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError("batched(): incomplete batch")
        yield batch
