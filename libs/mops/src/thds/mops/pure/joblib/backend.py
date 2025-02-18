"""A mops.pure powered joblib backend.

Additionally, several hacks to work around either bugs or bad behavior in joblib.

mops itself does _not_ force a dependency on joblib.

If you want to use this supplied helper class, you must ensure that
Joblib is installed, or you will get an ImportError.
"""

import typing as ty

from joblib._parallel_backends import LokyBackend, SequentialBackend, ThreadingBackend  # type: ignore

from thds.core import files, log

from ..core.types import Runner
from .batching import patch_joblib_parallel_batching  # noqa

logger = log.getLogger(__name__)


class NoMemmapLokyBackend(LokyBackend):
    """Workaround for joblib/Cython bug exposed via SciKit-Learn.

    https://github.com/scikit-learn/scikit-learn/issues/7981#issuecomment-341879166

    Since this only runs on the remotes, and only then a few
    CPUs/processes at a time, the extra memcopies being done are
    no big deal.
    """

    def configure(self, *args: ty.Any, **kwargs: ty.Any) -> None:
        kwargs["max_nbytes"] = None
        return super().configure(*args, **kwargs)


class TrillMlParallelJoblibBackend(ThreadingBackend):
    """A joblib backend that forwards to our MemoizingPicklingRunner.

    Performs simple batching based on the parallelism and oversubscribe factors at construction time.

    Note that you'll likely need to customize pre_dispatch, otherwise
    there won't be enough created tasks to actually batch anything.
    """

    supports_sharedmem = False
    uses_threads = False

    def __init__(
        self,
        runner: Runner,
        parallelism: int,
        n_cores: int,
        oversubscribe: int = 10,
    ):
        """number of cores should be the number of cores available on the remote system."""
        files.bump_limits()
        self.runner = runner
        self.n_cores = n_cores
        self._n_jobs = parallelism
        self.oversubscribe = oversubscribe

    def effective_n_jobs(self, _nj: int) -> int:
        return self._n_jobs

    def compute_batch_size(self) -> int:
        return self.n_cores * self.oversubscribe

    def apply_async(self, func: ty.Any, callback: ty.Any = None) -> ty.Any:
        def call_in_runner() -> ty.Any:
            return self.runner(func, (), dict())

        return super().apply_async(call_in_runner, callback=callback)

    def get_nested_backend(self) -> ty.Any:
        nesting_level = getattr(self, "nesting_level", 0) + 1
        if nesting_level > 1:
            logger.warning("Using sequential backend")
            return SequentialBackend(nesting_level=nesting_level), None
        return NoMemmapLokyBackend(nesting_level=nesting_level), self.n_cores
