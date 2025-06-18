"""Sometimes you want to require that a memoized result exists.

A Runner should hook into this system to enforce that upon itself.
"""

import os
import typing as ty
from contextlib import contextmanager

from thds.core import config, log, stack_context
from thds.termtool import colorize

from ..uris import lookup_blob_store

_REQUIRE_ALL_RESULTS = config.item("require_all_results", default="")
_UNLESS_ENV = stack_context.StackContext("results_unless_env", "")
# please do not set the above globally unless you really, truly know what you're doing.
logger = log.getLogger(__name__)
ORANGE = colorize.colorized("#FF8200")

_NO_MSG = "xxxx-REQUIRED-xxxx"


@contextmanager
def require_all(message: str = _NO_MSG, *, unless_env: str = "") -> ty.Iterator[None]:
    """Requires all results from this point down the stack, _unless_ the non-empty env
    variable specified is present in os.environ.

    An empty string message will still force results to be required.
    """
    with _REQUIRE_ALL_RESULTS.set_local(message or _NO_MSG), _UNLESS_ENV.set(unless_env):
        try:
            yield
        except RequiredResultNotFound as rrnf:
            # re-raising gives us a cleaner stack trace from the point where the result was required
            raise RequiredResultNotFound(rrnf.args[0], rrnf.uri) from rrnf


# _REQUIRED_FUNC_NAMES = set()


# def required(func: ty.Callable) -> None:
#     pass


def _should_require_result(memo_uri: str = "") -> str:
    requirement_msg = _REQUIRE_ALL_RESULTS()
    if not requirement_msg:
        return ""
    envvar_to_override = _UNLESS_ENV()
    is_envvar_set = envvar_to_override and envvar_to_override in os.environ
    if is_envvar_set:
        return ""
    if envvar_to_override:
        return f"{requirement_msg}; Note that you can set the environment variable {envvar_to_override} to skip this check."
    return requirement_msg


class Success(ty.NamedTuple):
    value_uri: str


class Error(ty.NamedTuple):
    exception_uri: str


RESULT = "result"
EXCEPTION = "exception"


class RequiredResultNotFound(Exception):
    def __init__(self, message: str, uri: str):
        super().__init__(message)
        self.uri = uri


def check_if_result_exists(
    memo_uri: str,
    rerun_excs: bool = False,
    before_raise: ty.Callable[[], ty.Any] = lambda: None,
) -> ty.Union[None, Success, Error]:
    fs = lookup_blob_store(memo_uri)
    value_uri = fs.join(memo_uri, RESULT)
    if fs.exists(value_uri):
        return Success(value_uri)

    required_msg = _should_require_result(memo_uri)
    if required_msg:  # might be custom or the default. either way it indicates a required result.
        before_raise()
        error_msg = f"Required a result for {ORANGE(memo_uri)} but that result was not found"
        # i'm tired of visually scanning for these memo_uris in logs.
        if required_msg != _NO_MSG:
            error_msg += f": {required_msg}"
        raise RequiredResultNotFound(error_msg, memo_uri)

    if rerun_excs:
        return None

    error_uri = fs.join(memo_uri, EXCEPTION)
    if fs.exists(error_uri):
        return Error(error_uri)

    return None
