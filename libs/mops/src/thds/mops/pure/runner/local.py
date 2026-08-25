"""Joins pickle functionality and Blob Store functionality to run functions remotely."""

import concurrent.futures
import contextvars
import typing as ty
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

from thds.core import concurrency, futures, log, scope
from thds.termtool.colorize import colorized, make_colorized_out

from ..._utils.on_slow import LogSlow, on_slow
from ...config import max_concurrent_network_ops
from .._futures import MopsFuture
from ..core import deferred_work, lease, memo, metadata
from ..core.lease.maintain import MAINTAIN_LEASES  # noqa: F401
from ..core.types import Args, Kwargs, T
from ..tools import console
from ..tools.summarize import run_summary
from . import lease_waiter, prepare, same_process_in_flight, strings, types
from .get_results import (
    PostShimResultGetter,
    ResultAndInvocationType,
    lease_maintaining_future,
    unwrap_value_or_error,
)

# this semaphore (and a similar one in get_results) allow us to prioritize getting a single unit
# of progress _complete_, rather than issuing many instructions to the
# underlying client and allowing it to randomly order the operations
# such that it takes longer to get a full unit of work complete.
# Reentrant for the same reason as above — a mops call during argument
# resolution can trigger another mops call on the same thread.
_BEFORE_INVOCATION_SEMAPHORE = concurrency.ReentrantBoundedSemaphore(int(max_concurrent_network_ops()))
# _BEFORE prioritizes uploading a single invocation and its dependencies so the Shim can start running.

_DarkBlue = colorized(fg="white", bg="#00008b")
_GreenYellow = colorized(fg="black", bg="#adff2f")
_Pink = colorized(fg="black", bg="#ff1493")
logger = log.getLogger(__name__)
_LogKnownResult = make_colorized_out(_DarkBlue, out=logger.info, fmt_str=" {} ")
_LogNewInvocation = make_colorized_out(_GreenYellow, out=logger.info, fmt_str=" {} ")
_LogInvocationAfterTakeover = make_colorized_out(_Pink, out=logger.info, fmt_str=" {} ")


def invoke_via_shim_or_return_memoized(  # noqa: C901
    serialize_args_kwargs: types.SerializeArgsKwargs,
    serialize_invocation: types.SerializeInvocation,
    shim_builder: types.ShimBuilder,
    get_meta_and_result: types.GetMetaAndResult,
    run_directory: ty.Optional[Path] = None,
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]] = dict(),  # noqa: B006
) -> ty.Callable[[bool, str, ty.Callable[..., T], Args, Kwargs], MopsFuture[T]]:
    @scope.bound
    def create_invocation_and_result_future(
        rerun_exceptions: bool,
        function_memospace: str,
        # by allowing the caller to set the function memospace, we allow 'redirects' to look up an old result by name.
        # while still guaranteeing that the function arguments were the same.
        func: ty.Callable[..., T],
        args_: Args,
        kwargs_: Kwargs,
    ) -> MopsFuture[T]:
        """This is the generic local runner. Its core abstractions are:

        - serializers of some sort (for the function and its arguments)
        - a runtime shim of some sort (can start a Python process somewhere else)
        - a result and metadata deserializer
        - URIs that are supported by a registered BlobStore implementation.

        It uses a mops-internal lease mechanism to prevent concurrent invocations for the same function+args.
        """
        invoked_at = datetime.now(tz=timezone.utc)
        # capture immediately, because many things may delay actual start.
        val_or_res = "value" if rerun_exceptions else "result"

        storage_root, fs, func, args, kwargs, pipeline_id, args_kwargs_bytes, memo_uri = (
            prepare.prepare_call(
                serialize_args_kwargs, calls_registry, function_memospace, func, args_, kwargs_
            )
        )

        # Define some important and reusable 'chunks of work'
        @on_slow(lambda s: LogSlow(f"check_if_result_exists took {s:.1f}s for {memo_uri}"))
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

        @on_slow(lambda s: LogSlow(f"acquire_lease took {s:.1f}s for {memo_uri}"))
        def acquire_lease() -> ty.Optional[lease.LeaseAcquired]:
            return lease.acquire(fs.join(memo_uri, lease.LEASE_DIRNAME), expire=timedelta(seconds=88))

        @on_slow(lambda s: LogSlow(f"upload_invocation_and_deps took {s:.1f}s for {memo_uri}"))
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

        def invoke_with_lease(
            lease_owned: lease.LeaseAcquired,
            log_invocation: ty.Callable[[str], ty.Any] = _LogNewInvocation,
        ) -> MopsFuture[T]:
            """The 'we own this invocation' path: upload the invocation, hand off to the
            runtime shim, and wrap the shim's answer in a future.

            Runs on the submitting thread when the lease is acquired on the first try; a
            lease_waiter takeover runs it inside a contextvars snapshot of that thread
            (see the lease-blocked branch below).
            """
            # once we own the lease, we are in 'run this ourselves' mode forever. If our
            # invocation fails, we fail, without any attempt to go 'back' to waiting for
            # someone else to compute the result.
            release_lease_in_current_process = lease.maintain_to_release(lease_owned)

            completion_signal: "concurrent.futures.Future[None]" = concurrent.futures.Future()
            same_process_in_flight.register(memo_uri, completion_signal)

            try:
                with _BEFORE_INVOCATION_SEMAPHORE:
                    log_invocation(f"Invoking {memo_uri}")
                    upload_invocation_and_deps()

                console.invoked(memo_uri, attempt_id=lease_owned.writer_id, at=invoked_at)
                # paired with the remote's 'started' - the interval between them is queue
                # wait (image pull, scheduling), which neither side can measure alone.

                # can't hold the semaphore while we block on the shim, though.
                shim = shim_builder(func, args_, kwargs_)
                future_or_shim_result = shim(  # ACTUAL INVOCATION (handoff to remote shim) HAPPENS HERE
                    (
                        memo_uri,
                        *metadata.format_invocation_cli_args(
                            metadata.InvocationMetadata.new(
                                pipeline_id,
                                invoked_at,
                                lease_owned.writer_id,
                                console.current_run_name(),
                            )
                        ),
                    )
                )

                future_result_getter = PostShimResultGetter[T](memo_uri, p_unwrap_value_or_error)
                if hasattr(future_or_shim_result, "add_done_callback"):
                    # if the shim returns a Future, we wrap it.
                    logger.debug("Shim returned a Future; wrapping it for post-shim result retrieval.")
                    # PostShimResultGetter.__call__ returns (value, metadata), so the lazy future
                    # yields that tuple type; we cast to make the type match from_tuple_future's sig.
                    lazy: futures.PFuture[tuple[T, ty.Optional[metadata.ResultMetadata]]] = ty.cast(
                        "futures.PFuture[tuple[T, ty.Optional[metadata.ResultMetadata]]]",
                        futures.make_lazy(lease_maintaining_future)(
                            lease_owned, future_result_getter, future_or_shim_result
                        ),
                    )
                    # lazy yields (value, metadata) since PostShimResultGetter now returns a tuple.
                    mops_future = MopsFuture.from_tuple_future(lazy, memo_uri)
                    same_process_in_flight.register(memo_uri, mops_future)  # replaces the placeholder
                    return mops_future

                else:  # it's a synchronous shim - just process the result directly.
                    future_result_getter.release_lease = release_lease_in_current_process
                    value, md = future_result_getter(future_or_shim_result)
                    f = MopsFuture(futures.resolved(value), memo_uri)
                    f.set_result_metadata(md)
                    return f

            except Exception as exc:
                console.emit(
                    console.failed(
                        memo_uri,
                        attempt_id=lease_owned.writer_id,
                        at=datetime.now(tz=timezone.utc),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
                # the remote never ran, or ran and reported nothing, so nothing else will
                # ever mark this invocation terminal. Without this it reads as invoked
                # forever - indistinguishable from work still waiting for a pod.
                try:
                    release_lease_in_current_process()
                except Exception:
                    logger.exception(
                        f"Failed to release lease {lease_owned.writer_id} after failed invocation."
                    )
                raise
            finally:
                if not completion_signal.done():
                    completion_signal.set_result(None)  # wake subscribers

        # the network ops being grouped by _BEFORE_INVOCATION include one or more
        # download attempts (consider possible Paths) plus
        # one or more uploads (embedded Paths & Sources/refs, and then invocation).
        with _BEFORE_INVOCATION_SEMAPHORE:
            # it's possible that our result may already exist from a previous run of this pipeline id.
            # we can short-circuit the entire process by looking for that result and returning it immediately.
            result = check_result_exists("memoized")
            if result:
                value, md = p_unwrap_value_or_error(memo_uri, result)
                f: MopsFuture[T] = MopsFuture(futures.resolved(value), memo_uri)
                f.set_result_metadata(md)
                return f

            lease_owned = acquire_lease()
            # if no result exists, the vastly most common outcome here will be acquiring
            # the lease on the first try.

        if lease_owned:
            return invoke_with_lease(lease_owned)

        # another caller holds the lease. Hand the wait to the shared waiter daemon and
        # return a pending future immediately, rather than parking this thread in a
        # sleep/check loop for however long the other caller takes.
        def check_awaited_result() -> ResultAndInvocationType | None:
            with _BEFORE_INVOCATION_SEMAPHORE:
                return check_result_exists("awaited")

        def acquire_lease_for_takeover() -> lease.LeaseAcquired | None:
            with _BEFORE_INVOCATION_SEMAPHORE:
                return acquire_lease()

        # The closures below run on other threads, after submit()'s stack-local contexts
        # (hashref map, args/kwargs, deferred uploads) have been torn down - so each runs
        # in a snapshot. Two, because a Context cannot be entered by two threads at once.
        polling_context = contextvars.copy_context()
        takeover_context = contextvars.copy_context()

        return MopsFuture.from_tuple_future(
            lease_waiter.future_awaiting_lease(
                memo_uri,
                what=val_or_res,
                check_result=lambda: polling_context.run(check_awaited_result),
                unwrap=lambda result: polling_context.run(p_unwrap_value_or_error, memo_uri, result),
                acquire_lease=lambda: polling_context.run(acquire_lease_for_takeover),
                invoke_with_lease=lambda lease_owned: takeover_context.run(
                    invoke_with_lease, lease_owned, log_invocation=_LogInvocationAfterTakeover
                ),
            ),
            memo_uri,
        )

    return create_invocation_and_result_future
