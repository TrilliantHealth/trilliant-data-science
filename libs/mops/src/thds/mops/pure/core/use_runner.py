"""use_runner wrapper decorator factory lives here.

You can transfer control to a Runner without this, but decorators are a Pythonic approach.
"""

import typing as ty
from functools import wraps

from thds.core import log, stack_context

from .types import Runner

USE_RUNNER_BYPASS = stack_context.StackContext("use_runner_bypass", False)
# use this in a Runner remote entry point to allow the remote function call
# to bypass any use_runner decorator. Also necessary in case somebody is doing advanced
# things like using a remote runner to run a manifest of _other_ remote functions...

logger = log.getLogger(__name__)
F = ty.TypeVar("F", bound=ty.Callable)


def use_runner(runner: Runner, skip: ty.Callable[[], bool] = lambda: False) -> ty.Callable[[F], F]:
    """Wrap a function that is pure with respect to its arguments and result.

    Run that function on the provided runner.

    The arguments must be able to be transmitted by the runner to the
    remote context and not refer to anything that will not be
    accessible in that context.
    """

    def deco(f: F) -> F:
        @wraps(f)
        def __use_runner_wrapper(*args, **kwargs):  # type: ignore
            if USE_RUNNER_BYPASS() or skip():
                logger.debug("Calling function %s directly...", f)
                with USE_RUNNER_BYPASS.set(False):
                    return f(*args, **kwargs)

            logger.debug("Forwarding local function %s call to runner...", f)
            return runner(f, args, kwargs)

        return ty.cast(F, __use_runner_wrapper)

    return deco
