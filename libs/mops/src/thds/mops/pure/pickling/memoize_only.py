"""A tiny shim 'shell' that supports invoking a pickle-memoized function
directly in the current thread. This is a good thing to use when the
computation derives no advantage from not running locally
(i.e. exhibits no parallelism) but you still want memoization.
"""

from typing import Callable, Sequence

from thds.core.log import getLogger

from ..core import use_runner
from ..core.entry.runner_registry import run_named_entry_handler
from ..core.types import F
from ..core.uris import UriResolvable
from .mprunner import MemoizingPicklingRunner

logger = getLogger(__name__)


def _threadlocal_shell(shell_args: Sequence[str]) -> None:
    """Use this inside a memoizing Runner to get the memoization
    without needing to transfer control to an external process.
    """
    logger.info("Running a use_runner function locally in the current thread.")
    run_named_entry_handler(*shell_args)


def memoize_in(uri_resolvable: UriResolvable) -> Callable[[F], F]:
    """A decorator that makes a function globally-memoizable, but running in the current
    thread.

    This is a good thing to use when the computation derives no
    advantage from not running locally (i.e. exhibits no parallelism)
    but you still want memoization.

    This enables nested memoized function calls, which is not (yet)
    the default for `use_runner`.
    """
    return use_runner(MemoizingPicklingRunner(_threadlocal_shell, uri_resolvable))
