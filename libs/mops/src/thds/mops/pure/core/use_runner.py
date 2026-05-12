"""use_runner wrapper decorator factory lives here.

You can transfer control to a Runner without this, but decorators are a Pythonic approach.
"""

import typing as ty
from contextlib import contextmanager
from functools import wraps

from thds.core import log, stack_context
from thds.mops._utils.names import full_name_and_callable

from .types import Runner

# A stack of "the next wrapped call for this function should bypass the runner"
# tokens. `unwrap_use_runner(f)` pushes `full_name(f)`. A wrapper takes the
# bypass path only when its own full_name matches the top of the stack, and
# pops the token on match — so the bypass is one-shot, scoped to the function
# it was opened for. This makes recursive @pure.magic re-enter the runner on
# sub-calls (the stack is empty for them) while still letting an outer mops
# function's body call inner mops functions through normal dispatch (their
# names don't match the top token).
_UNWRAP_STACK = stack_context.StackContext[tuple[str, ...]]("use_runner_unwrap_stack", ())
_CONSUMED_TOP = stack_context.StackContext[bool]("use_runner_consumed_top", False)

logger = log.getLogger(__name__)
F = ty.TypeVar("F", bound=ty.Callable)


@contextmanager
def unwrap_use_runner(f: F) -> ty.Iterator[None]:
    """Tell the next wrapped call for ``f`` to bypass the runner and run its body directly.

    The runner's remote entry point uses this so the just-deserialized invocation
    actually executes (rather than being re-submitted). The bypass is keyed to
    ``f``'s identity and one-shot: once a wrapped call for ``f`` consumes it,
    further calls (recursive or otherwise) re-enter the runner normally.
    """
    full_name, _ = full_name_and_callable(f)
    with _UNWRAP_STACK.set(_UNWRAP_STACK() + (full_name,)), _CONSUMED_TOP.set(False):
        yield


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
            if skip():
                logger.debug("Calling function %s directly (skip)...", f)
                return f(*args, **kwargs)

            stack = _UNWRAP_STACK()
            if stack and not _CONSUMED_TOP():
                full_name, _ = full_name_and_callable(f)
                if full_name == stack[-1]:
                    logger.debug("Calling function %s directly (bypass)...", f)
                    with _CONSUMED_TOP.set(True):
                        return f(*args, **kwargs)

            logger.debug("Forwarding local function %s call to runner...", f)
            return runner(f, args, kwargs)

        return ty.cast(F, __use_runner_wrapper)

    return deco
