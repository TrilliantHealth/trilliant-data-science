import concurrent.futures
import subprocess
from typing import Sequence

from thds.core import log

from ..core.entry.runner_registry import MOPS_EXCEPTION_EXIT_CODE, run_named_entry_handler

logger = log.getLogger(__name__)


def samethread_shim(shim_args: Sequence[str]) -> None:
    """Use this inside a memoizing Runner to get the memoization
    without needing to transfer control to an external process.
    """
    logger.debug("Running a mops function locally in the current thread.")
    run_named_entry_handler(*shim_args)


def subprocess_shim(shim_args: Sequence[str]) -> None:
    logger.debug("Running a mops function locally in a new subprocess.")
    try:
        subprocess.check_call(["python", "-m", "thds.mops.pure.core.entry.main", *shim_args])
    except subprocess.CalledProcessError as e:
        if e.returncode != MOPS_EXCEPTION_EXIT_CODE:
            raise
        # user function raised an exception; it was serialized to blob storage before exit.
        # return normally so the caller can retrieve it via the standard result-reading path.


def future_subprocess_shim(shim_args: Sequence[str]) -> concurrent.futures.Future:
    """Use this if you really want a Future rather than just running the process"""
    logger.debug("Running a mops function in a new subprocess, returning a Future.")
    return concurrent.futures.ProcessPoolExecutor().submit(samethread_shim, shim_args)
