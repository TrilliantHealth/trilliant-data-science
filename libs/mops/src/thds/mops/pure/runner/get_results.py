import threading
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from ...config import max_concurrent_network_ops
from ..core import memo
from ..core.types import NoResultAfterShimSuccess
from ..tools.summarize import run_summary
from . import types


class ResultAndInvocationType(ty.NamedTuple):
    value_or_error: ty.Union[memo.results.Success, memo.results.Error]
    invoc_type: run_summary.InvocationType


def unwrap_value_or_error(
    get_meta_and_result: types.GetMetaAndResult,
    run_directory: ty.Optional[Path],
    runner_prefix: str,
    args_kwargs_uris: ty.Collection[str],
    release_lock: ty.Callable[[], None],
    memo_uri: str,
    result_and_itype: ResultAndInvocationType,
) -> ty.Any:  # the result value
    result = result_and_itype.value_or_error
    metadata = None
    value_t = None
    try:
        if isinstance(result, memo.results.Success):
            metadata, value_t = get_meta_and_result("value", result.value_uri)
            return value_t
        else:
            assert isinstance(result, memo.results.Error), "Must be Error or Success"
            metadata, exc = get_meta_and_result("EXCEPTION", result.exception_uri)
            raise exc
    finally:
        run_summary.log_function_execution(
            *(run_directory, memo_uri, result_and_itype.invoc_type),
            metadata=metadata,
            runner_prefix=runner_prefix,
            was_error=not isinstance(result, memo.results.Success),
            return_value=value_t,
            args_kwargs_uris=args_kwargs_uris,
        )
        release_lock()


_AFTER_INVOCATION_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _IN prioritizes retrieving the result of a Shim that has completed.

T = ty.TypeVar("T")


@dataclass(frozen=True)
class PostShimResultGetter(ty.Generic[T]):
    """Must be serializable on its own, so we can pass it across process boundaries
    to serve as a foundation for a cross-process Future.

    Happily, this should not be terribly difficult, as the 'state' of a mops function
    is predicted entirely on the memo URI, which is a string.
    """

    partially_applied_unwrap_value_or_error: ty.Callable[
        [ty.Callable[[], None], str, ResultAndInvocationType], T
    ]
    release_lock: ty.Callable[[], None]
    memo_uri: str

    def __call__(self, _shim_result: ty.Any) -> T:
        """Check if the result exists, and return it if it does."""
        memo_uri = self.memo_uri

        with _AFTER_INVOCATION_SEMAPHORE:
            value_or_error = memo.results.check_if_result_exists(memo_uri, check_for_exception=True)
            if not value_or_error:
                raise NoResultAfterShimSuccess(
                    f"The shim for {memo_uri} exited cleanly, but no result or exception was found."
                )
            return self.partially_applied_unwrap_value_or_error(
                self.release_lock, memo_uri, ResultAndInvocationType(value_or_error, "invoked")
            )
