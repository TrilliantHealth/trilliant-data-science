import subprocess
from typing import Sequence

from thds.core import log

from ..core.entry.runner_registry import run_named_entry_handler

logger = log.getLogger(__name__)


def samethread_shim(shim_args: Sequence[str]) -> None:
    """Use this inside a memoizing Runner to get the memoization
    without needing to transfer control to an external process.
    """
    logger.debug("Running a mops function locally in the current thread.")
    run_named_entry_handler(*shim_args)


def subprocess_shim(shim_args: Sequence[str]) -> None:
    logger.debug("Running a mops function locally in a new subprocess.")
    subprocess.check_call(["python", "-m", "thds.mops.pure.core.entry.main", *shim_args])
