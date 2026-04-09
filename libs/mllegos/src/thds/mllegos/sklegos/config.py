import contextlib
import os
import typing as ty

import joblib

from thds.core import log

_LOGGER = log.getLogger(__name__)

# number of threads available to C/C++ code compiled with OpenMP
OMP_NUM_THREADS = "OMP_NUM_THREADS"
# number of threads available to Intel MKL optimization libs
MKL_NUM_THREADS = "MKL_NUM_THREADS"
# number of threads available to linear algebra libs
OPENBLAS_NUM_THREADS = "OPENBLAS_NUM_THREADS"
BLIS_NUM_THREADS = "BLIS_NUM_THREADS"

# See https://scikit-learn.org/stable/computing/parallelism.html#lower-level-parallelism-with-openmp
# for more detail on each of these


def _set_env(name: str, value: str, action: str):
    if (set_value := os.getenv(name)) != value:
        _LOGGER.warning(f"{action} env var {name} from {set_value!r} to {value!r}")
    os.environ[name] = value


def configure(num_cores: int, backend: str = "threading") -> joblib.parallel_config:
    """Configure this library for use in an application.

    :param num_cores: the number of cores to be made available for inference. Useful in an application that needs
        to devote some resources to other tasks. For instance, where inference may be running in a
        multiprocessing context using all available cores, it may be useful to set this to 1 to prevent
        contention.
    """
    for env_var in (OMP_NUM_THREADS, MKL_NUM_THREADS, OPENBLAS_NUM_THREADS, BLIS_NUM_THREADS):
        _set_env(env_var, str(num_cores), "Overriding")

    return joblib.parallel_config(backend=backend, n_jobs=num_cores)


# https://stackoverflow.com/questions/2059482/temporarily-modify-the-current-processs-environment
@contextlib.contextmanager
def configure_local(
    num_cores: int, backend: str = "threading"
) -> ty.Generator[joblib.parallel_config, None, None]:
    """Temporarily set the configuration variables inside a context manager. It's "local" in the sense
    of applying only locally to a block of code; it is *not* thread-local; should be called from the main
    process/thread, and the config will be inherited by all subprocesses/threads. All involved values
    will be reset to their original state on exit. See `configure` for more documentation"""
    old_environ = dict(os.environ)
    joblib_config = configure(num_cores, backend)
    try:
        with joblib_config:
            yield joblib_config
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
