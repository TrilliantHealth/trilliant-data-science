import os
import typing as ty
from contextlib import contextmanager

from thds.mops import pure

_DEFAULT_MESSAGE = "Required memoized results not found. Run the failing test locally to materialize the results for use in CI"


@contextmanager
def require_all_in_ci(message: str = _DEFAULT_MESSAGE) -> ty.Iterator[None]:
    """Context manager that enforces memoized results exist in CI, but allows
    local execution to proceed without them."""
    if "CI" in os.environ:
        with pure.results.require_all(message):
            yield
    else:
        yield
