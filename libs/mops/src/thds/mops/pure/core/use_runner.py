"""use_runner wrapper decorator factory lives here.

You can transfer control to a Runner without this, but decorators are a Pythonic approach.
"""

import typing as ty
from functools import wraps

from thds.core import log, stack_context

from .entry.runner_registry import entry_count
from .types import Runner

logger = log.getLogger(__name__)
F = ty.TypeVar("F", bound=ty.Callable)
FUNCTION_UNWRAP_COUNT = stack_context.StackContext("function_unwrap_count", 0)


def _is_runner_entry() -> bool:
    """Function is being called in the context of a Runner."""
    return entry_count() > FUNCTION_UNWRAP_COUNT()


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
            if _is_runner_entry() or skip():
                logger.debug("Calling function %s directly...", f)
                with FUNCTION_UNWRAP_COUNT.set(FUNCTION_UNWRAP_COUNT() + 1):
                    return f(*args, **kwargs)

            logger.debug("Forwarding local function %s call to runner...", f)
            return runner(f, args, kwargs)

        return ty.cast(F, __use_runner_wrapper)

    return deco
