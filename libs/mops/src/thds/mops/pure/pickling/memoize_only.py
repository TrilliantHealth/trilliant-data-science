from typing import Callable

from ..core.types import F
from ..core.uris import UriResolvable
from ..core.use_runner import use_runner
from ..runner import simple_shims
from .mprunner import MemoizingPicklingRunner


# this may soon become deprecated in favor of mops.pure.magic(blob_root=...)
def memoize_in(uri_resolvable: UriResolvable) -> Callable[[F], F]:
    """A decorator that makes a function globally-memoizable, but running in the current
    thread.

    This is a good thing to use when the computation derives no
    advantage from not running locally (i.e. exhibits no parallelism)
    but you still want memoization.

    This enables nested memoized function calls, which is not (yet)
    the default for `use_runner`.
    """
    return use_runner(MemoizingPicklingRunner(simple_shims.samethread_shim, uri_resolvable))
