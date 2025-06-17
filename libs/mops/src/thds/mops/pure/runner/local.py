"""Joins pickle functionality and Blob Store functionality to run functions remotely.
"""

import threading
import time
import typing as ty
from datetime import datetime, timedelta, timezone
from pathlib import Path

from thds.core import config, log, scope

from ..._utils.colorize import colorized, make_colorized_out
from ...config import max_concurrent_network_ops
from ..core import deferred_work, lock, memo, metadata, pipeline_id_mask, uris
from ..core.partial import unwrap_partial
from ..core.types import Args, Kwargs, NoResultAfterShimSuccess, T
from ..tools.summarize import run_summary
from . import strings, types

MAINTAIN_LOCKS = config.item("thds.mops.pure.local.maintain_locks", default=True, parse=config.tobool)

# these two semaphores allow us to prioritize getting meaningful units
# of progress _complete_, rather than issuing many instructions to the
# underlying client and allowing it to randomly order the operations
# such that it takes longer to get a full unit of work complete.
_BEFORE_INVOCATION_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _OUT prioritizes uploading a single invocation and its dependencies so the Shim can start running.
_AFTER_INVOCATION_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _IN prioritizes retrieving the result of a Shim that has completed.

_DarkBlue = colorized(fg="white", bg="#00008b")
_GreenYellow = colorized(fg="black", bg="#adff2f")
_Purple = colorized(fg="white", bg="#800080")
logger = log.getLogger(__name__)
_LogKnownResult = make_colorized_out(_DarkBlue, out=logger.info, fmt_str=" {} ")
_LogNewInvocation = make_colorized_out(_GreenYellow, out=logger.info, fmt_str=" {} ")
_LogAwaitedResult = make_colorized_out(_Purple, out=logger.info, fmt_str=" {} ")


def invoke_via_shim_or_return_memoized(  # noqa: C901
    serialize_args_kwargs: types.SerializeArgsKwargs,
    serialize_invocation: types.SerializeInvocation,
    shim_builder: types.ShimBuilder,
    get_meta_and_result: types.GetMetaAndResult,
    run_directory: ty.Optional[Path] = None,
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]] = dict(),  # noqa: B006
) -> ty.Callable[[bool, str, ty.Callable[..., T], Args, Kwargs], T]:
    @scope.bound
    def create_invocation__check_result__wait_shim(
        rerun_exceptions: bool,
        function_memospace: str,
        # by allowing the caller to set the function memospace, we allow 'redirects' to look up an old result by name.
        # while still guaranteeing that the function arguments were the same.
        func: ty.Callable[..., T],
        args_: Args,
        kwargs_: Kwargs,
    ) -> T:
        """This is the generic local runner. Its core abstractions are:

        - serializers of some sort (for the function and its arguments)
        - a runtime shim of some sort (can start a Python process somewhere else)
        - a result and metadata deserializer
        - URIs that are supported by a registered BlobStore implementation.

        It uses a mops-internal locking mechanism to prevent concurrent invocations for the same function+args.
        """
        invoked_at = datetime.now(tz=timezone.utc)
        # capture immediately, because many things may delay actual start.
        storage_root = uris.get_root(function_memospace)
        scope.enter(uris.ACTIVE_STORAGE_ROOT.set(storage_root))
        fs = uris.lookup_blob_store(function_memospace)
        val_or_res = "value" if rerun_exceptions else "result"

        # we need to unwrap any partial object and combine its wrapped
        # args, kwargs with the provided args, kwargs, otherwise the
        # args and kwargs will not get properly considered in the memoization key.
        func, args, kwargs = unwrap_partial(func, args_, kwargs_)
        pipeline_id = scope.enter(pipeline_id_mask.including_function_docstr(func))
        # TODO pipeline_id should probably be passed in explicitly

        scope.enter(deferred_work.open_context())  # optimize Source objects during serialization

        args_kwargs_bytes = serialize_args_kwargs(storage_root, func, args, kwargs)
        memo_uri = fs.join(
            function_memospace,
            *memo.calls.combine_function_logic_keys(memo.calls.resolve(calls_registry, func)),
            # ^ these will embedded as extra nesting.
            memo.args_kwargs_content_address(args_kwargs_bytes),
        )

        # Define some important and reusable 'chunks of work'

        class ResultAndInvocationType(ty.NamedTuple):
            value_or_error: ty.Union[memo.results.Success, memo.results.Error]
            invoc_type: run_summary.InvocationType

        def check_result(
            invoc_type: run_summary.InvocationType,
        ) -> ty.Union[ResultAndInvocationType, None]:
            result = memo.results.check_if_result_exists(
                memo_uri, rerun_excs=rerun_exceptions, before_raise=debug_required_result_failure
            )
            if not result:
                return None

            _LogKnownResult(
                f"{invoc_type} {val_or_res} for {memo_uri} already exists and is being returned without invocation!"
            )
            return ResultAndInvocationType(result, invoc_type)

        def unwrap_value_or_error(result_and_itype: ResultAndInvocationType) -> T:
            result = result_and_itype.value_or_error
            metadata = None
            value_t = None
            try:
                if isinstance(result, memo.results.Success):
                    metadata, value_t = get_meta_and_result("value", result.value_uri)
                    return ty.cast(T, value_t)
                else:
                    assert isinstance(result, memo.results.Error), "Must be Error or Success"
                    metadata, exc = get_meta_and_result("EXCEPTION", result.exception_uri)
                    raise exc
            finally:
                run_summary.log_function_execution(
                    *(run_directory, memo_uri, result_and_itype.invoc_type),
                    metadata=metadata,
                    runner_prefix=function_memospace.split(pipeline_id)[0],
                    was_error=not isinstance(result, memo.results.Success),
                    return_value=value_t,
                    args_kwargs=(args, kwargs),
                )

        def acquire_lock() -> ty.Optional[lock.LockAcquired]:
            return lock.acquire(fs.join(memo_uri, "lock"), expire=timedelta(seconds=88))

        def upload_invocation_and_deps() -> None:
            # we're just about to transfer to a remote context,
            # so it's time to perform any deferred work
            deferred_work.perform_all()

            fs.putbytes(
                fs.join(memo_uri, strings.INVOCATION),
                serialize_invocation(storage_root, func, args_kwargs_bytes),
                type_hint="application/mops-invocation",
            )

        def debug_required_result_failure() -> None:
            # This is entirely for the purpose of making debugging easier. It serves no internal functional purpose.
            #
            # first, upload the invocation as an accessible marker of what was expected to exist.
            upload_invocation_and_deps()
            # then use mops-inspect programmatically to print the IRE in the same format as usual.
            from thds.mops.pure.tools.inspect import inspect_and_log

            inspect_and_log(memo_uri)

        # the network ops being grouped by _BEFORE_INVOCATION include one or more
        # download attempts (consider possible Paths) plus
        # one or more uploads (embedded Paths & Sources/refs, and then invocation).
        with _BEFORE_INVOCATION_SEMAPHORE:
            # now actually execute the chunks of work that are required...

            # it's possible that our result may already exist from a previous run of this pipeline id.
            # we can short-circuit the entire process by looking for that result and returning it immediately.
            result = check_result("memoized")
            if result:
                return unwrap_value_or_error(result)

            lock_owned = acquire_lock()
            # if no result exists, the vastly most common outcome here will be acquiring
            # the lock on the first try.  this will lead to breaking out of
            # the LOCK LOOP directly below and going on to the shim invocation.
            # still, we release the semaphore b/c we can't sleep while holding a lock.

        # LOCK LOOP: entering this loop (where we attempt to acquire the lock) is the common non-memoized case
        while not result:
            if lock_owned:
                if MAINTAIN_LOCKS():
                    release_lock = lock.launch_daemon_lock_maintainer(lock_owned)
                else:
                    release_lock = lock_owned.release
                break  # we own the invocation - invoke the shim ourselves (below)

            # getting to this point ONLY happens if we failed to acquire the lock, which
            # is not expected to be the usual situation. We log a differently-colored
            # message here to make that clear to users.
            _LogAwaitedResult(
                f"{val_or_res} for {memo_uri} does not exist, but the lock is owned by another process."
            )
            time.sleep(22)

            with _BEFORE_INVOCATION_SEMAPHORE:
                result = check_result("awaited")
                if result:
                    _LogAwaitedResult(
                        f"{val_or_res} for {memo_uri} was found after waiting for the lock."
                    )
                    return unwrap_value_or_error(result)

                lock_owned = acquire_lock()  # still inside the semaphore, as it's a network op

        assert release_lock is not None
        assert lock_owned is not None
        # if/when we acquire the lock, we move forever into 'run this ourselves mode'.
        # If something about our invocation fails,
        # we fail just as we would have previously, without any attempt to go
        # 'back' to waiting for someone else to compute the result.

        try:
            with _BEFORE_INVOCATION_SEMAPHORE:
                _LogNewInvocation(f"Invoking {memo_uri}")
                upload_invocation_and_deps()

            # can't hold the semaphore while we block on the shim, though.
            shim_ex = None
            shim = shim_builder(func, args_, kwargs_)
            shim(  # ACTUAL INVOCATION (handoff to remote shim) HAPPENS HERE
                (
                    memo_uri,
                    *metadata.format_invocation_cli_args(
                        metadata.InvocationMetadata.new(pipeline_id, invoked_at, lock_owned.writer_id)
                    ),
                )
            )
        except Exception as ex:
            # network or similar errors are very common and hard to completely eliminate.
            # We know that if a result (or error) exists, then the network failure is
            # not important, because results in blob storage are atomically populated (either fully there or not)
            logger.exception("Error awaiting shim. Optimistically checking for result.")
            shim_ex = ex

        finally:
            release_lock()

        # the network ops being grouped by _AFTER_INVOCATION include one or more downloads.
        with _AFTER_INVOCATION_SEMAPHORE:
            value_or_error = memo.results.check_if_result_exists(memo_uri)
            if not value_or_error:
                if shim_ex:
                    raise shim_ex  # re-raise the underlying exception rather than making up our own.
                raise NoResultAfterShimSuccess(
                    f"The shim for {memo_uri} exited cleanly, but no result or exception was found."
                )
            return unwrap_value_or_error(ResultAndInvocationType(value_or_error, "invoked"))

    return create_invocation__check_result__wait_shim
