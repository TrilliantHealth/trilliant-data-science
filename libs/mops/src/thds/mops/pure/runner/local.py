"""Joins pickle functionality and Blob Store functionality to run functions remotely."""

import threading
import time
import typing as ty
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

from thds.core import futures, log, scope
from thds.termtool.colorize import colorized, make_colorized_out

from ...config import max_concurrent_network_ops
from ..core import deferred_work, lock, memo, metadata, pipeline_id_mask, uris
from ..core.lock.maintain import MAINTAIN_LOCKS  # noqa: F401
from ..core.partial import unwrap_partial
from ..core.types import Args, Kwargs, T
from ..tools.summarize import run_summary
from . import strings, types
from .get_results import (
    PostShimResultGetter,
    ResultAndInvocationType,
    lock_maintaining_future,
    unwrap_value_or_error,
)

# this semaphore (and a similar one in get_results) allow us to prioritize getting a single unit
# of progress _complete_, rather than issuing many instructions to the
# underlying client and allowing it to randomly order the operations
# such that it takes longer to get a full unit of work complete.
_BEFORE_INVOCATION_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _BEFORE prioritizes uploading a single invocation and its dependencies so the Shim can start running.

_DarkBlue = colorized(fg="white", bg="#00008b")
_GreenYellow = colorized(fg="black", bg="#adff2f")
_Purple = colorized(fg="white", bg="#800080")
_Pink = colorized(fg="black", bg="#ff1493")
logger = log.getLogger(__name__)
_LogKnownResult = make_colorized_out(_DarkBlue, out=logger.info, fmt_str=" {} ")
_LogNewInvocation = make_colorized_out(_GreenYellow, out=logger.info, fmt_str=" {} ")
_LogInvocationAfterSteal = make_colorized_out(_Pink, out=logger.info, fmt_str=" {} ")
_LogAwaitedResult = make_colorized_out(_Purple, out=logger.info, fmt_str=" {} ")


def invoke_via_shim_or_return_memoized(  # noqa: C901
    serialize_args_kwargs: types.SerializeArgsKwargs,
    serialize_invocation: types.SerializeInvocation,
    shim_builder: types.ShimBuilder,
    get_meta_and_result: types.GetMetaAndResult,
    run_directory: ty.Optional[Path] = None,
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]] = dict(),  # noqa: B006
) -> ty.Callable[[bool, str, ty.Callable[..., T], Args, Kwargs], futures.PFuture[T]]:
    @scope.bound
    def create_invocation_and_result_future(
        rerun_exceptions: bool,
        function_memospace: str,
        # by allowing the caller to set the function memospace, we allow 'redirects' to look up an old result by name.
        # while still guaranteeing that the function arguments were the same.
        func: ty.Callable[..., T],
        args_: Args,
        kwargs_: Kwargs,
    ) -> futures.PFuture[T]:
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
        def check_result_exists(
            invoc_type: run_summary.InvocationType,
        ) -> ty.Union[ResultAndInvocationType, None]:
            result = memo.results.check_if_result_exists(
                memo_uri,
                check_for_exception=not rerun_exceptions,
                before_raise=debug_required_result_failure,
            )
            if not result:
                return None

            _LogKnownResult(
                f"{invoc_type} {val_or_res} for {memo_uri} already exists and is being returned without invocation!"
            )
            return ResultAndInvocationType(result, invoc_type)

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

        p_unwrap_value_or_error = partial(
            unwrap_value_or_error,
            get_meta_and_result,
            run_directory,
            function_memospace.split(pipeline_id)[0],  # runner_prefix
            run_summary.extract_source_uris((args, kwargs)),
        )

        log_invocation = _LogNewInvocation  # this is what we use unless we steal the lock.

        # the network ops being grouped by _BEFORE_INVOCATION include one or more
        # download attempts (consider possible Paths) plus
        # one or more uploads (embedded Paths & Sources/refs, and then invocation).
        with _BEFORE_INVOCATION_SEMAPHORE:
            # now actually execute the chunks of work that are required...

            # it's possible that our result may already exist from a previous run of this pipeline id.
            # we can short-circuit the entire process by looking for that result and returning it immediately.
            result = check_result_exists("memoized")
            if result:
                return futures.resolved(p_unwrap_value_or_error(memo_uri, result))

            lock_owned = acquire_lock()
            # if no result exists, the vastly most common outcome here will be acquiring
            # the lock on the first try.  this will lead to breaking out of
            # the LOCK LOOP directly below and going on to the shim invocation.
            # still, we release the semaphore b/c we can't sleep while holding a lock.

        # LOCK LOOP: entering this loop (where we attempt to acquire the lock) is the common non-memoized case
        while not result:
            if lock_owned:
                break  # we own the invocation - invoke the shim ourselves (below)

            # getting to this point ONLY happens if we failed to acquire the lock, which
            # is not expected to be the usual situation. We log a differently-colored
            # message here to make that clear to users.
            _LogAwaitedResult(
                f"{val_or_res} for {memo_uri} does not exist, but the lock is owned by another process."
            )
            time.sleep(22)

            with _BEFORE_INVOCATION_SEMAPHORE:
                result = check_result_exists("awaited")
                if result:
                    _LogAwaitedResult(
                        f"{val_or_res} for {memo_uri} was found after waiting for the lock."
                    )
                    return futures.resolved(p_unwrap_value_or_error(memo_uri, result))

                lock_owned = acquire_lock()  # still inside the semaphore, as it's a network op
                if lock_owned:
                    log_invocation = _LogInvocationAfterSteal
                    logger.info(f"Stole expired lock for {memo_uri} - invoking ourselves.")

        assert lock_owned is not None
        # if/when we acquire the lock, we move forever into 'run this ourselves mode'.
        # If something about our invocation fails,
        # we fail just as we would have previously, without any attempt to go
        # 'back' to waiting for someone else to compute the result.
        release_lock_in_current_process = lock.maintain_to_release(lock_owned)

        try:
            with _BEFORE_INVOCATION_SEMAPHORE:
                log_invocation(f"Invoking {memo_uri}")
                upload_invocation_and_deps()

            # can't hold the semaphore while we block on the shim, though.
            shim = shim_builder(func, args_, kwargs_)
            future_or_shim_result = shim(  # ACTUAL INVOCATION (handoff to remote shim) HAPPENS HERE
                (
                    memo_uri,
                    *metadata.format_invocation_cli_args(
                        metadata.InvocationMetadata.new(pipeline_id, invoked_at, lock_owned.writer_id)
                    ),
                )
            )

            future_result_getter = PostShimResultGetter[T](memo_uri, p_unwrap_value_or_error)
            if hasattr(future_or_shim_result, "add_done_callback"):
                # if the shim returns a Future, we wrap it.
                logger.debug("Shim returned a Future; wrapping it for post-shim result retrieval.")
                return futures.make_lazy(lock_maintaining_future)(
                    lock_owned, future_result_getter, future_or_shim_result
                )

            else:  # it's a synchronous shim - just process the result directly.
                future_result_getter.release_lock = release_lock_in_current_process
                return futures.resolved(future_result_getter(future_or_shim_result))

        except Exception:
            try:
                release_lock_in_current_process()
            except Exception:
                logger.exception(
                    f"Failed to release lock {lock_owned.writer_id} after failed invocation."
                )
            raise

    return create_invocation_and_result_future
