"""A tiny shim 'shell' that supports invoking a memoized function
directly in the current thread. This is a good thing to use when the
computation derives no advantage from not running locally
(i.e. exhibits no parallelism) but you still want memoization.
"""
from typing import Callable, Optional, Sequence

from thds.core.log import getLogger

from ._registry import MAIN_HANDLER_BASE_ARGS
from ._uris import UriResolvable
from .core import F, pure_remote
from .main import run_main

logger = getLogger(__name__)


def direct_shell(shell_args: Sequence[str]) -> None:
    """Use this inside a memoizing Runner to get the memoization
    without needing to transfer control to an external process.
    """
    logger.info("Running a pure_remote function locally in the current thread.")
    n_base_args = len(MAIN_HANDLER_BASE_ARGS)
    assert tuple(shell_args[:n_base_args]) == MAIN_HANDLER_BASE_ARGS, (
        shell_args,
        MAIN_HANDLER_BASE_ARGS,
    )
    run_main(*shell_args[n_base_args:])


# TODO rename to something like memoize_shared or memoize_global,
# since it's storing results in a shared (ADLS) location.
def memoize_direct(uri_resolvable: Optional[UriResolvable] = None) -> Callable[[F], F]:
    """A decorator that makes a function memoizable in the current
    thread.
    This is a good thing to use when the computation derives no
    advantage from not running locally (i.e. exhibits no parallelism)
    but you still want memoization.

    This enables nested memoized function calls, which is not (yet)
    the default for `pure_remote`.
    """
    from ._backward_compat import AdlsPickleRunner

    return pure_remote(
        AdlsPickleRunner(direct_shell, uri_resolvable, rerun_exceptions=True), allow_nested=True
    )
