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
    # ... but the job does not exist yet! So we need to change some stuff under the hood I think.
    #
    # We could create that job name here and immediately return the future... and _then_
    # toss the args into the queue. the reason we still need the queue is because we need
    # to make sure that there's something that will 'see' the very last item that comes
    # through, even though we don't know which one that will be.
    #
    # if we _did_ know exactly how many items we were going to process, we could just do this 'manually'.
    # but we _don't_ know that, because some of the items may never make it to this point, since they will turn out
    # to have been resolved by mops prior to this point.
    #
    # Therefore, what we need is to have a context that tells us what the job name will be, and then we put that into
    # the queue alongside the rest of the args. On the other side, we can have a simple batching consumer
    # that recognizes when the job name has _changed_, and batches all the items that shared the same job name into that job.
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
