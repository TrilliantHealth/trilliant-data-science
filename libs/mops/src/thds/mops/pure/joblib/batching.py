"""Fix joblib batching for use with many parallel tasks running remotely."""

import itertools
import queue
import typing as ty
import unittest.mock
from collections import defaultdict
from contextlib import contextmanager

import joblib  # type: ignore
from joblib.parallel import BatchedCalls  # type: ignore

from thds.core.log import getLogger

logger = getLogger(__name__)


def dispatch_one_batch(self: ty.Any, iterator: ty.Iterable[ty.Any]) -> bool:
    """Joblib batching is truly horrible for running on a remote machine.

    Various things conspire to essentially try to outsmart your
    backend's batching instructions, and you end up with much smaller
    batches than desired if you try to launch lots of runtimes in
    parallel.

    This is an ugly monkey patch, but it _works_.
    """
    if not hasattr(self, "__patch_stats"):
        self.__patch_stats = defaultdict(int)  # type: ignore

    batch_size = self._backend.compute_batch_size()
    with self._lock:
        try:
            tasks = self._ready_batches.get(block=False)
        except queue.Empty:
            n_jobs = self._cached_effective_n_jobs
            islice = list(itertools.islice(iterator, batch_size * n_jobs))
            if len(islice) == 0:
                return False
            self.__patch_stats["tasks"] += len(islice)
            logger.info(
                f"Creating new tasks with patched batch size {batch_size}; "
                f"stats so far: {self.__patch_stats}"
            )
            for i in range(0, len(islice), batch_size):
                self._ready_batches.put(
                    BatchedCalls(
                        islice[i : i + batch_size],
                        self._backend.get_nested_backend(),
                        self._reducer_callback,
                        self._pickle_cache,
                    )
                )
            # finally, get one task.
            tasks = self._ready_batches.get(block=False)

        if len(tasks) == 0:
            return False
        self.__patch_stats["batches"] += 1
        self._dispatch(tasks)
        return True


@contextmanager
def patch_joblib_parallel_batching() -> ty.Iterator[None]:
    with unittest.mock.patch.object(joblib.Parallel, "dispatch_one_batch", dispatch_one_batch):
        yield
