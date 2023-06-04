"""A tiny shim 'shell' that supports invoking a memoized function
directly in the current thread. This is a good thing to use when the
computation derives no advantage from not running locally
(i.e. exhibits no parallelism) but you still want memoization.
"""
from typing import Sequence

from thds.core.log import getLogger

from ._registry import MAIN_HANDLER_BASE_ARGS, main_handler
from ._root import _IS_REMOTE
from .temp import _REMOTE_TMP

logger = getLogger(__name__)


def direct_shell(shell_args: Sequence[str]) -> None:
    """Use this inside a memoizing Runner to get the memoization
    without needing to transfer control to an external process.
    """
    logger.info("Running a pure_remote function locally in the current thread.")
    assert tuple(shell_args[:3]) == MAIN_HANDLER_BASE_ARGS, (shell_args, MAIN_HANDLER_BASE_ARGS)
    try:
        with _IS_REMOTE.set(True):
            main_handler(*shell_args[3:])
    finally:
        _REMOTE_TMP.cleanup()
