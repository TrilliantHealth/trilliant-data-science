import os
import typing as ty
from contextlib import contextmanager

from thds.mops import pure

_DEFAULT_MESSAGE = "Required memoized results not found. Run the failing test locally to materialize the results for use in CI"


def _require_all_in_ci(message: str = _DEFAULT_MESSAGE) -> ty.Iterator[None]:
    """Enforces memoized results exist in CI, but allows
    local execution to proceed without them."""
    if "CI" in os.environ:
        with pure.results.require_all(message):
            yield
    else:
        yield


require_all_in_ci = contextmanager(_require_all_in_ci)
