"""Sometimes you want to require that a memoized result exists.

A Runner should hook into this system to enforce that upon itself.
"""

import typing as ty
from contextlib import contextmanager

from thds.core import config, log

from ..uris import lookup_blob_store

_REQUIRE_ALL_RESULTS = config.item("require_all_results", default=False, parse=config.tobool)
# please do not set the above globally unless you really, truly know what you're doing.
logger = log.getLogger(__name__)


@contextmanager
def require_all() -> ty.Iterator[None]:
    with _REQUIRE_ALL_RESULTS.set_local(True):
        yield


def all_required() -> bool:
    return _REQUIRE_ALL_RESULTS()


# _REQUIRED_FUNC_NAMES = set()


# def required(func: ty.Callable) -> None:
#     pass


def _require_result(memo_uri: str = "") -> bool:
    return all_required()


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
    result_uri = fs.join(memo_uri, RESULT)
    if fs.exists(result_uri):
        return Success(result_uri)

    if _require_result(memo_uri):
        before_raise()
        raise RequiredResultNotFound(
            f"Required a result for {memo_uri} but that result was not found", memo_uri
        )

    if rerun_excs:
        return None

    error_uri = fs.join(memo_uri, EXCEPTION)
    if fs.exists(error_uri):
        return Error(error_uri)

    return None
