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
import itertools
import threading
import typing as ty

from thds.core import futures, log

from . import _launch, counts

T = ty.TypeVar("T")
logger = log.getLogger(__name__)


class _AtExitBatcher(ty.Generic[T]):
    def __init__(self, batch_processor: ty.Callable[[ty.Collection[T]], None]) -> None:
        self.batch: list[T] = []
        self._registered = False
        self._lock = threading.RLock()
        self._batch_processor = batch_processor

    def add(self, item: T) -> None:
        with self._lock:
            if not self._registered:
                atexit.register(self.process)
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
        name_prefix: str = "",
    ) -> None:
        """submit_func in particular should be a closure around whatever setup you need to
        do to call back into a function that is locally wrapped with a k8s shim that will
        ultimately call k8s.launch. Notably, you
        """
        super().__init__(self._process_batch)
        self._max_batch_size = max_batch_size
        self._job_counter = job_counter
        self._job_name = ""
        self._name_prefix = name_prefix
        self._submit_func = submit_func

    def _get_new_name(self) -> str:
        with self._job_counter.get_lock():
            self._job_counter.value += 1
            return _launch.construct_job_name(self._name_prefix, f"{self._job_counter.value:0>3}")

    def add_to_named_job(self, mops_invocation: ty.Sequence[str]) -> str:
        """Returns job name for the invocation."""
        with self._lock:
            if not self._job_name:
                self._job_name = self._get_new_name()
            if len(self.batch) >= self._max_batch_size:
                self.process()
                self._job_name = self._get_new_name()
            super().add(" ".join(mops_invocation))
            return self._job_name

    def _process_batch(self, batch: ty.Collection[str]) -> None:
        with _launch.JOB_NAME.set(self._job_name):
            log_lvl = logger.warning if len(batch) < self._max_batch_size else logger.info
            log_lvl(f"Processing batch of len {len(batch)} with job name {self._job_name}")
            self._submit_func(batch)


F = ty.TypeVar("F", bound=ty.Callable)
FunctionDecorator = ty.Callable[[F], F]


_BATCHER: ty.Optional[K8sJobBatchingShim] = None


def init_batcher(
    submit_func: ty.Callable[[ty.Collection[str]], ty.Any],
    func_max_batch_size: int,
    job_counter: counts.MpValue[int],
    name_prefix: str = "",
) -> None:
    # for use with multiprocessing pool initializer
    global _BATCHER
    if _BATCHER is not None:
        logger.warning("Batcher is already initialized; reinitializing will reset the job name.")
        return

    _BATCHER = K8sJobBatchingShim(submit_func, func_max_batch_size, job_counter, name_prefix)


def init_batcher_with_unpicklable_submit_func(
    make_submit_func: ty.Callable[[T], ty.Callable[[ty.Collection[str]], ty.Any]],
    submit_func_arg: T,
    func_max_batch_size: int,
    job_counter: counts.MpValue[int],
    name_prefix: str = "",
) -> None:
    return init_batcher(
        make_submit_func(submit_func_arg),
        func_max_batch_size,
        job_counter,
        name_prefix=name_prefix,
    )


def shim(args: ty.Sequence[str]) -> futures.PFuture[bool]:
    # This thing needs to return a lazy Uncertain Future that contains a job name, so that Job can be polled on
    # ... but the job does not exist yet! So the batcher is in charge of creating the job name
    # upfront, and then ensuring that it gets used when the job is created.
    assert _BATCHER is not None, "Batcher must be initialized before using the batching shim."
    job_name = _BATCHER.add_to_named_job(args)
    return _launch.create_lazy_job_logging_future(job_name)


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
