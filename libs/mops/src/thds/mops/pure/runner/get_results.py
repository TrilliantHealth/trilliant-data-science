import concurrent.futures
import typing as ty
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from thds.core import concurrency, futures, log

from ...config import max_concurrent_network_ops
from ..core import lease, memo
from ..core import metadata as metadata_mod
from ..core.types import NoResultAfterShimSuccess
from ..tools import console
from ..tools.summarize import run_summary
from . import types


class ResultAndInvocationType(ty.NamedTuple):
    value_or_error: ty.Union[memo.results.Success, memo.results.Error]
    invoc_type: run_summary.InvocationType


def _iso(metadata: ty.Any, field: str) -> str:
    """A metadata timestamp as ISO8601, or empty - the metadata travels with the pickle
    and may predate the field entirely."""
    value = getattr(metadata, field, None)
    return value.isoformat() if isinstance(value, datetime) else ""


def unwrap_value_or_error(
    get_meta_and_result: types.GetMetaAndResult,
    run_directory: ty.Optional[Path],
    runner_prefix: str,
    args_kwargs_uris: ty.Collection[str],
    memo_uri: str,
    result_and_itype: ResultAndInvocationType,
) -> tuple[ty.Any, ty.Optional[metadata_mod.ResultMetadata]]:  # (value, metadata)
    result = result_and_itype.value_or_error
    metadata = None
    value_t = None
    try:
        if isinstance(result, memo.results.Success):
            metadata, value_t = get_meta_and_result("value", result.value_uri)
            return value_t, metadata
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
        if result_and_itype.invoc_type in ("memoized", "awaited"):
            console.memoized(
                memo_uri,
                at=datetime.now(tz=timezone.utc),
                was_error=not isinstance(result, memo.results.Success),
                invoked_at=_iso(metadata, "invoked_at"),
                started_at=_iso(metadata, "remote_started_at"),
                ended_at=_iso(metadata, "remote_ended_at"),
                run_name=getattr(metadata, "console_run_name", ""),
            )
            # this run never invoked it, so nothing else will ever report it - and to a
            # remote observer the invocation would simply not exist. An awaited result is
            # a memoized one this run had to wait for: computed under another
            # orchestrator's lease, whose remote reported to that run, not this one.


_AFTER_INVOCATION_SEMAPHORE = concurrency.ReentrantBoundedSemaphore(
    int(max_concurrent_network_ops()) * 3
)
# _IN prioritizes retrieving the result of a Shim that has completed.
logger = log.getLogger(__name__)
T = ty.TypeVar("T")


@dataclass
class PostShimResultGetter(ty.Generic[T]):
    """Must be serializable on its own, so we can pass it across process boundaries
    to serve as a foundation for a cross-process Future.

    Happily, this should not be terribly difficult, as the 'state' of a mops function
    is predicted entirely on the memo URI, which is a string.
    """

    memo_uri: str
    partially_applied_unwrap_value_or_error: ty.Callable[
        [str, ResultAndInvocationType],
        tuple[T, ty.Optional[metadata_mod.ResultMetadata]],
    ]
    release_lease: ty.Optional[ty.Callable[[], None]] = None

    def __call__(self, _shim_result: ty.Any) -> tuple[T, ty.Optional[metadata_mod.ResultMetadata]]:
        """Check if the result exists, and return it if it does.

        This is the future 'translator' that allows us to chain a shim future to be a result future.
        """
        memo_uri = self.memo_uri

        try:
            with _AFTER_INVOCATION_SEMAPHORE:
                value_or_error = memo.results.check_if_result_exists(memo_uri, check_for_exception=True)
                if not value_or_error:
                    console.emit(
                        console.failed(
                            memo_uri,
                            attempt_id="",
                            at=datetime.now(tz=timezone.utc),
                            error="shim exited cleanly but wrote no result",
                        )
                    )
                    # the shim succeeded, so the done-callback treats this as a success and
                    # never fires. Nothing else will mark this terminal.
                    raise NoResultAfterShimSuccess(
                        f"The shim for {memo_uri} exited cleanly, but no result or exception was found."
                    )
                return self.partially_applied_unwrap_value_or_error(
                    memo_uri, ResultAndInvocationType(value_or_error, "invoked")
                )
        finally:
            if self.release_lease is not None:
                try:
                    self.release_lease()
                except Exception:
                    logger.exception("Failed to release lease after shim result retrieval.")


def _release_lease_on_failure(
    release_lease: ty.Callable[[], None],
    shim_future: futures.PFuture,
    memo_uri: str = "",
    attempt_id: str = "",
) -> None:
    """The chained result getter (whose finally releases the lease) only runs when the
    shim future succeeds. A failed or cancelled shim future would otherwise leave the
    lease maintained until process exit - never expiring, so every retry of the
    invocation (including this process's own) would wait on it forever."""
    error = ""
    try:
        exc = shim_future.exception()
        error = f"{type(exc).__name__}: {exc}" if exc is not None else ""
    except BaseException as cancelled:  # noqa: B036 - cancellation also means the getter never runs
        error = f"{type(cancelled).__name__}: {cancelled}"

    if not error:
        return

    if memo_uri:
        console.emit(
            console.failed(
                memo_uri,
                attempt_id=attempt_id,
                at=datetime.now(tz=timezone.utc),
                error=error,
            )
        )
        # this is where a dead pod surfaces: the shim future fails, or completes with no
        # result written, and no remote is left to report either. The invocation would
        # otherwise sit in `invoked` for the rest of the run.

    try:
        release_lease()
    except Exception:
        logger.exception("Failed to release lease after failed shim future.")


def lease_maintaining_future(
    lease_acquired: lease.LeaseAcquired,
    post_shim_result_getter: PostShimResultGetter[futures.R1],
    inner_future: futures.PFuture[futures.R],
) -> concurrent.futures.Future[futures.R1]:
    """Create a Future that will be used to retrieve the result of a shim invocation.

    Most commonly, this will be partially applied and only lazily invoked
    when the user calls `.result()` or some other method on the Future.

    This Future will be used to retrieve the result of a shim invocation, and will
    maintain the lease while it is being retrieved.
    """
    release_lease = lease.maintain_to_release(lease_acquired)
    post_shim_result_getter.release_lease = release_lease
    inner_future.add_done_callback(
        lambda fut: _release_lease_on_failure(
            release_lease, fut, post_shim_result_getter.memo_uri, lease_acquired.writer_id
        )
    )
    return futures.chain_futures(inner_future, post_shim_result_getter)
