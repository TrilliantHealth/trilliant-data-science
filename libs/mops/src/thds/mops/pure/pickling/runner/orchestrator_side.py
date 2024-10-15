"""Joins pickle functionality and Blob Store functionality to run functions remotely.
"""

import inspect
import threading
import time
import typing as ty
from datetime import timedelta
from functools import lru_cache
from pathlib import Path

from thds.core import config, log, scope

from ...._utils.colorize import colorized
from ...._utils.once import Once
from ....config import max_concurrent_network_ops
from ....srcdest.destf_pointers import trigger_dest_files_placeholder_write
from ....srcdest.srcf_trigger_upload import trigger_src_files_upload
from ...core import deferred_work, lock, memo, uris
from ...core.partial import unwrap_partial
from ...core.pipeline_id_mask import function_mask
from ...core.serialize_big_objs import ByIdRegistry, ByIdSerializer
from ...core.serialize_paths import CoordinatingPathSerializer
from ...core.types import Args, F, Kwargs, NoResultAfterInvocationError, Serializer, T
from ...tools.summarize import run_summary
from .._pickle import (
    Dumper,
    SourceArgumentPickler,
    freeze_args_kwargs,
    gimme_bytes,
    make_read_object,
    wrap_f,
)
from ..pickles import NestedFunctionPickle
from . import sha256_b64

MAINTAIN_LOCKS = config.item(
    "thds.mops.pure.orchestrator.maintain_locks", default=True, parse=config.tobool
)

Shell = ty.Callable[[ty.Sequence[str]], ty.Any]
"""A Shell is a way of getting back into a Python process with enough
context to download the uploaded function and its arguments from the
location where a runner placed it, and then invoke the function. All
arguments are strings because it is assumed that this represents some
kind of command line invocation.

The Shell must be a blocking call, and its result(s) must be available
immediately after its return.
"""


class ShellBuilder(ty.Protocol):
    def __call__(self, __f: F, __args: Args, __kwargs: Kwargs) -> Shell:
        ...  # pragma: no cover


def _mk_builder(shell: ty.Union[Shell, ShellBuilder]) -> ShellBuilder:
    """If you have a Shell and you want to make it into the simplest possible ShellBuilder."""

    if len(inspect.signature(shell).parameters) == 3:
        return ty.cast(ShellBuilder, shell)

    def static_shell_builder(_f: F, _args: Args, _kwargs: Kwargs) -> Shell:
        return ty.cast(Shell, shell)

    return ty.cast(ShellBuilder, static_shell_builder)


logger = log.getLogger(__name__)
RUNNER_SUFFIX = "mops2-mpf"  # namespace we use in case there are later incompatibility issues.
Redirect = ty.Callable[[F, Args, Kwargs], F]
NO_REDIRECT = lambda f, _args, _kwargs: f  # noqa: E731


def _runner_prefix_for_pickled_functions(storage_root: str) -> str:
    return uris.lookup_blob_store(storage_root).join(storage_root, RUNNER_SUFFIX)


class MemoizingPicklingRunner:
    """
    Runs callables in a process as defined by the Shell.
    This is often a remote process, however a local shell may be provided.
    """

    def __init__(
        self,
        shell: ty.Union[ShellBuilder, Shell],
        blob_storage_root: uris.UriResolvable,
        *,
        rerun_exceptions: bool = True,
        serialization_registry: ByIdRegistry[ty.Any, Serializer] = ByIdRegistry(),  # noqa: B008
        redirect: Redirect = NO_REDIRECT,
    ):
        """Construct a memoizing shell runner.

        Transmitted Path resources will be content-hash-addressed
        below the runner_prefix to save storage and increase chances
        of memoization. Named objects will be treated
        similarly. Function invocations will be pickled and stored
        under the current pipeline id since we do not have a way of
        inferring whether their associated code (or embedded
        Src/DestFile objects) are safely content-addressable across
        runs.

        The Shell must forward control in the remote environment to a
        wrapper that will pull the function and arguments from the URI(s).

        A ShellBuilder will receive the original function and its
        original arguments, which you can use to determine which
        concrete Shell implementation to return for the given function
        call.

        `rerun_exceptions` will cause a pre-existing `exception`
        result to be ignored, as though Exceptions in your function
        are the result of transient errors and not an expected return
        value of a (simulated) pure function. If you do not want this
        behavior, turn it off.

        `redirect` changes only the function that is actually invoked
        on the remote side of the runner. It does not change the
        computed memoization key, which is still based on the original
        function and the args, kwargs pair passed in. A common use for
        this would be allowing a contextually-aware function to be
        invoked in the manner of initializer/initargs, without those
        additional bits being part of the function invocation and
        therefore the memoization key, especially where they're not
        picklable at all.
        """
        self._shell_builder = _mk_builder(shell)
        self._get_storage_root = uris.to_lazy_uri(blob_storage_root)
        self._rerun_exceptions = rerun_exceptions
        self._by_id_registry = serialization_registry
        self._redirect = redirect

        self._run_directory = run_summary.create_mops_run_directory()

    def shared(self, *objs: ty.Any, **named_objs: ty.Any):
        """Set up memoizing pickle serialization for these objects.

        Provided names are used for debugging purposes only.
        """
        for obj in objs:
            self._by_id_registry[obj] = sha256_b64.Sha256B64Pickler()
        for name, obj in named_objs.items():
            self._by_id_registry[obj] = sha256_b64.Sha256B64Pickler(name)

    # LRU cache is not thread-safe in Python and I should replace this.
    @lru_cache(maxsize=None)  # noqa: B019
    def _get_stateful_dumper(self, _root: str) -> Dumper:
        """We want one of these per blob storage root, because the
        invocation and result must exist on the same blob store as
        any other automatically dumped objects, e.g. Paths or named
        objects, such that the full invocation payload is
        byte-for-byte identical, since its hash is our memoization
        key.
        """
        return Dumper(
            ByIdSerializer(self._by_id_registry),
            CoordinatingPathSerializer(sha256_b64.Sha256B64PathStream(), Once()),
            SourceArgumentPickler(),
        )

    def __call__(self, f: ty.Callable[..., T], args: Args, kwargs: Kwargs) -> T:
        """Return result of running this function remotely via the shell.

        Passes data to shell process via pickles in a Blob Store.

        May return cached (previously-computed) results found via the
        derived function memo URI, which contains the determinstic
        hashed bytes of all the function arguments, but also
        additional namespacing including pipeline_id as documented
        in memo.function_memospace.py.
        """
        logger.debug("Preparing to run function via remote shell")
        return _pickle_func_and_run_via_shell(
            memo.make_function_memospace(
                _runner_prefix_for_pickled_functions(self._get_storage_root()), f
            ),
            self._get_stateful_dumper,
            f,
            self._run_directory,
        )(self._shell_builder, self._rerun_exceptions, self._redirect, f, args, kwargs)


# these two semaphores allow us to prioritize getting meaningful units
# of progress _complete_, rather than issuing many instructions to the
# underlying client and allowing it to randomly order the operations
# such that it takes longer to get a full unit of work complete.
_BEFORE_INVOCATION_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _OUT prioritizes uploading a single invocation and its dependencies so the Shell can start running.
_AFTER_INVOCATION_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _IN prioritizes retrieving the result of a Shell that has completed.

INVOCATION = "invocation"
_DarkBlue = colorized(fg="white", bg="#00008b")
_GreenYellow = colorized(fg="black", bg="#adff2f")
_Purple = colorized(fg="white", bg="#800080")
_LogKnownResult = lambda s: logger.info(_DarkBlue(s))  # noqa: E731
_LogNewInvocation = lambda s: logger.info(_GreenYellow(s))  # noqa: E731
_LogAwaitedResult = lambda s: logger.info(_Purple(s))  # noqa: E731


def _pickle_func_and_run_via_shell(  # noqa: C901
    function_memospace: str,
    get_dumper: ty.Callable[[str], Dumper],
    func_: ty.Callable[..., T],
    run_directory: ty.Optional[Path] = None,
) -> ty.Callable[[ShellBuilder, bool, Redirect, ty.Callable, Args, Kwargs], T]:
    storage_root = uris.get_root(function_memospace)

    @scope.bound
    def run_shell_via_pickles_(
        shell_builder: ShellBuilder,
        rerun_exceptions: bool,
        remote_redirect: Redirect,
        func: ty.Callable,
        args_: Args,
        kwargs_: Kwargs,
    ) -> T:
        scope.enter(uris.ACTIVE_STORAGE_ROOT.set(storage_root))
        fs = uris.lookup_blob_store(function_memospace)
        dumper = get_dumper(storage_root)

        # the network ops being grouped by _BEFORE_INVOCATION include one or more
        # download attempts (consider possible pickled Paths) plus
        # one or more uploads (embedded Paths, invocation).
        with _BEFORE_INVOCATION_SEMAPHORE:
            # we need to unwrap the partial object and combine its
            # args, kwargs with the other args, kwargs, otherwise the
            # args and kwargs will not get properly considered in the
            # memoization key.
            func, args, kwargs = unwrap_partial(func_, args_, kwargs_)
            pipeline_id = scope.enter(function_mask(func))

            trigger_src_files_upload(args, kwargs)
            # eagerly upload (deprecated) SrcFiles prior to serialization - this is slow and
            # it will lovely to get rid of it soon-ish.
            scope.enter(deferred_work.open_context())
            # prepare to optimize Source objects during serialization
            args_kwargs_bytes = freeze_args_kwargs(dumper, func, args, kwargs)  # serialize!
            memo_uri = fs.join(function_memospace, memo.args_kwargs_content_address(args_kwargs_bytes))

            # Define some important and reusable 'chunks of work'
            # - these are ordered by what they depend on, _not_ the order
            #   in which they'll be called.

            def upload_pickled_invocation_and_deps():
                # we're just about to transfer to a remote context,
                # so it's time to perform any deferred work,
                # so that our shells don't have to be aware of this.
                deferred_work.perform_all()

                fs.putbytes(
                    fs.join(memo_uri, INVOCATION),  # until v3, continue to use no suffix here.
                    gimme_bytes(
                        dumper,
                        NestedFunctionPickle(
                            wrap_f(remote_redirect(func, args, kwargs)), args_kwargs_bytes
                        ),
                    ),
                    type_hint="application/mops-invocation",
                )

            def unwrap_remote_result(result: ty.Union[memo.results.Success, memo.results.Error]) -> T:
                if isinstance(result, memo.results.Success):
                    success = ty.cast(T, make_read_object("result")(result.value_uri))
                    trigger_dest_files_placeholder_write(success)
                    return success
                assert isinstance(result, memo.results.Error), "Must be _error or _success"
                raise make_read_object("EXCEPTION")(result.exception_uri)

            def debug_required_result_failure():
                """This is entirely for the purpose of making debugging easier. It serves no internal functional purpose."""
                # first, upload the invocation as an accessible marker of what was expected to exist.
                upload_pickled_invocation_and_deps()
                # then use mops-inspect programmatically to print the IRE in the same format as usual.
                from thds.mops.pure.tools.inspect import inspect

                inspect(memo_uri)
                logger.error(
                    "A required result was not found."
                    " You can compare the above output with other invocations"
                    f" by running `mops-inspect {memo_uri}`"
                    " in your local Python environment."
                )

            def check_result(
                status: run_summary.StatusType,
            ) -> ty.Union[memo.results.Success, memo.results.Error, None]:
                result = memo.results.check_if_result_exists(
                    memo_uri, rerun_excs=rerun_exceptions, before_raise=debug_required_result_failure
                )
                if not result:
                    return None

                _LogKnownResult(
                    f"{status} result for {memo_uri} already exists"
                    " and is being returned without invocation!"
                )
                if run_directory:
                    run_summary.log_function_execution(
                        run_directory, func_, memo_uri, status=status, memospace=function_memospace
                    )
                return result

            # now actually execute the chunks of work that are required...

            # it's possible that our result may already exist from a previous run of this pipeline id.
            # we can short-circuit the entire process by looking for that result and returning it immediately.
            result = check_result("memoized")
            if result:
                return unwrap_remote_result(result)

        # but if it does not exist, let's:
        # - exit the BEFORE_INVOCATION_SEMAPHORE
        # - grab the lock and try to run it ourselves
        # - or wait for someone else to run it if it's already in progress.

        lock_dir_uri = fs.join(memo_uri, "lock")
        # entering this loop is the most common case - the non-memoized case.
        while not result:
            with _BEFORE_INVOCATION_SEMAPHORE:
                lock_owned = lock.acquire(lock_dir_uri, expire=timedelta(seconds=88))
                # the vastly most common outcome here will be acquiring the lock on the
                # first try.  this will lead to (directly below) breaking out of the loop
                # and going on to the shell invocation.

            # relinquish semaphore before we sleep, so that other threads can acquire it.
            # we could relinquish after the if lock_owned stuff, but that stuff doesn't do
            # any ADLS operations anyway, so we may as well give it up now.

            if lock_owned:
                if MAINTAIN_LOCKS():
                    release_lock = lock.launch_daemon_lock_maintainer(lock_owned)
                else:
                    release_lock = lock_owned.release
                break  # we own the invocation - invoke the shell ourselves

            # getting to this point ONLY happens if we failed to acquire the lock, which
            # is not expected to be the usual situation. We log a differently-colored
            # message here to make that clear to users.
            _LogAwaitedResult(
                f"Result for {memo_uri} does not yet exist but the lock is owned by another process."
            )
            time.sleep(22)

            with _BEFORE_INVOCATION_SEMAPHORE:
                result = check_result("awaited")

        if result:
            # I don't think this needs to be inside a semaphore, because the 'load' here
            # should be much less spiky, given that it will only happen after failing to
            # acquire the lock the first time.
            _LogAwaitedResult(f"Result for {memo_uri} was found after waiting for the lock.")
            return unwrap_remote_result(result)

        assert release_lock is not None
        # if/when we acquire the lock, we move forever into
        # 'run this ourselves mode' - if something about our invocation fails,
        # we fail just as we would have previously, without any attempt to go
        # 'back' to waiting for someone else to compute the result.

        try:
            _LogNewInvocation(f"Triggering new invocation for {memo_uri}")
            with _BEFORE_INVOCATION_SEMAPHORE:
                upload_pickled_invocation_and_deps()

            shell_ex = None
            shell = shell_builder(func, args_, kwargs_)

            shell(
                (
                    MemoizingPicklingRunner.__name__,
                    memo_uri,
                    pipeline_id,  # for debugging only
                )
            )
        except Exception as ex:
            # network or similar errors are very common and hard to completely eliminate.
            # We know that if a result (or error) exists, then the network failure is
            # not important, because results in ADLS are atomically populated (either fully there or not).
            logger.exception(
                "Caught error when awaiting shell result. Optimistically checking for final result on ADLS."
            )
            shell_ex = ex
        finally:
            release_lock()

        # the network ops being grouped by _AFTER_INVOCATION include one or more downloads.
        with _AFTER_INVOCATION_SEMAPHORE:
            result = memo.results.check_if_result_exists(memo_uri)
            if not result:
                if shell_ex:
                    raise shell_ex  # re-raise the underlying exception rather than making up our own.
                raise NoResultAfterInvocationError(memo_uri)
            if run_directory:
                # Log that the function was executed
                run_summary.log_function_execution(
                    run_directory, func_, memo_uri, status="invoked", memospace=function_memospace
                )
            return unwrap_remote_result(result)

    return run_shell_via_pickles_
