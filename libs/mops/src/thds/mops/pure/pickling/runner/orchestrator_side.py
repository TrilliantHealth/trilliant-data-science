"""Joins pickle functionality and Blob Store functionality to run functions remotely.
"""
import inspect
import threading
import typing as ty
from functools import lru_cache

from thds.core import log, scope

from ....__about__ import backward_compatible_with
from ...._utils.colorize import colorized
from ...._utils.once import Once
from ....config import max_concurrent_network_ops
from ....srcdest.destf_pointers import trigger_dest_files_placeholder_write
from ....srcdest.srcf_trigger_upload import trigger_src_files_upload
from ...core import uris
from ...core.memo import args_kwargs_content_address, make_function_memospace
from ...core.pipeline_id_mask import get_pipeline_id_mask
from ...core.serialize_big_objs import ByIdRegistry, ByIdSerializer
from ...core.serialize_paths import CoordinatingPathSerializer
from ...core.types import Args, F, Kwargs, NoResultAfterInvocationError, Serializer, T
from .._pickle import Dumper, freeze_args_kwargs, gimme_bytes, make_read_object, wrap_f
from ..pickles import NestedFunctionPickle
from . import sha256_b64

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
        ...


def _mk_builder(shell: ty.Union[Shell, ShellBuilder]) -> ShellBuilder:
    """If you have a Shell and you want to make it into the simplest possible ShellBuilder."""

    if len(inspect.signature(shell).parameters) == 3:
        return ty.cast(ShellBuilder, shell)

    def static_shell_builder(_f: F, _args: Args, _kwargs: Kwargs) -> Shell:
        return ty.cast(Shell, shell)

    return ty.cast(ShellBuilder, static_shell_builder)


logger = log.getLogger(__name__)
RUNNER_SUFFIX = f"mops{backward_compatible_with()}-mpf"
Redirect = ty.Callable[[F, Args, Kwargs], F]
NO_REDIRECT = lambda f, _args, _kwargs: f  # noqa: E731


def _runner_prefix_for_pickled_functions(storage_root: str) -> str:
    return uris.lookup_blob_store(storage_root).join(storage_root, RUNNER_SUFFIX)


class MemoizingPicklingRunner:
    """Runs callables in a remote process as defined by the Shell."""

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
        """
        self._shell_builder = _mk_builder(shell)
        self._get_storage_root = uris.to_lazy_uri(blob_storage_root)
        self._rerun_exceptions = rerun_exceptions
        self._by_id_registry = serialization_registry
        self._redirect = redirect

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
            make_function_memospace(_runner_prefix_for_pickled_functions(self._get_storage_root()), f),
            self._get_stateful_dumper,
            f,
        )(self._shell_builder(f, args, kwargs), self._rerun_exceptions, self._redirect, args, kwargs)


# these two semaphores allow us to prioritize getting meaningful units
# of progress _complete_, rather than issuing many instructions to the
# underlying client and allowing it to randomly order the operations
# such that it takes longer to get a full unit of work complete.
_OUT_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _OUT prioritizes uploading a single invocation and its dependencies so the Shell can start running.
_IN_SEMAPHORE = threading.BoundedSemaphore(int(max_concurrent_network_ops()))
# _IN prioritizes retrieving the result of a Shell that has completed.

INVOCATION = "invocation"
RESULT = "result"
EXCEPTION = "exception"
_DarkBlue = colorized(fg="white", bg="#00008b")
_GreenYellow = colorized(fg="black", bg="#adff2f")
_LogKnownResult = lambda s: logger.info(_DarkBlue(s))  # noqa: E731
_LogPrepareNewInvocation = lambda s: logger.info(_GreenYellow(s))  # noqa: E731


def _pickle_func_and_run_via_shell(
    function_memospace: str,
    get_dumper: ty.Callable[[str], Dumper],
    func: ty.Callable[..., T],
) -> ty.Callable[[Shell, bool, Redirect, Args, Kwargs], T]:
    storage_root = uris.get_root(function_memospace)

    @scope.bound
    def run_shell_via_pickles_(
        shell: Shell,
        rerun_exceptions: bool,
        redirect: Redirect,
        args: Args,
        kwargs: Kwargs,
    ) -> T:
        scope.enter(uris.ACTIVE_STORAGE_ROOT.set(storage_root))
        fs = uris.lookup_blob_store(function_memospace)
        dumper = get_dumper(storage_root)

        # the network ops being grouped by _OUT include one or more
        # download attempts (consider possible pickled Paths) plus
        # one or more uploads (embedded Paths, invocation).
        with _OUT_SEMAPHORE:
            trigger_src_files_upload(args, kwargs)
            args_kwargs_bytes = freeze_args_kwargs(dumper, func, args, kwargs)
            memo_uri = fs.join(function_memospace, args_kwargs_content_address(args_kwargs_bytes))

            class _success(ty.NamedTuple):
                success_uri: str

            class _error(ty.NamedTuple):
                exception_uri: str

            def fetch_result_if_exists(
                rerun_excs: bool = False,
            ) -> ty.Union[None, _success, _error]:
                result_uri = fs.join(memo_uri, RESULT)
                if fs.exists(result_uri):
                    return _success(result_uri)
                if rerun_excs:
                    return None
                error_uri = fs.join(memo_uri, EXCEPTION)
                if fs.exists(error_uri):
                    return _error(error_uri)
                return None

            def unwrap_remote_result(result: ty.Union[_success, _error]) -> T:
                if isinstance(result, _success):
                    success = ty.cast(T, make_read_object(RESULT)(result.success_uri))
                    trigger_dest_files_placeholder_write(success)
                    return success
                assert isinstance(result, _error), "Must be _error or _success"
                raise make_read_object("EXCEPTION")(result.exception_uri)

            # it's possible that our result may already exist from a previous run of this pipeline id.
            # we can short-circuit the entire process by looking for that result and returning it immediately.
            result = fetch_result_if_exists(rerun_excs=rerun_exceptions)
            if result:
                _LogKnownResult(
                    f"Result for {memo_uri} already exists and is being returned without invocation!"
                )
                return unwrap_remote_result(result)
            _LogPrepareNewInvocation(f"Preparing new remote invocation for {memo_uri}")
            # but if it does not exist, we need to upload the invocation and then run the shell.
            fs.putbytes(
                fs.join(memo_uri, INVOCATION),
                gimme_bytes(
                    dumper,
                    NestedFunctionPickle(wrap_f(redirect(func, args, kwargs)), args_kwargs_bytes),
                ),
                type_hint=INVOCATION,
            )

        try:
            shell_ex = None
            shell(
                (
                    MemoizingPicklingRunner.__name__,
                    memo_uri,
                    get_pipeline_id_mask(),  # for debugging only
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

        # the network ops being grouped by _IN include one or more downloads.
        with _IN_SEMAPHORE:
            result = fetch_result_if_exists()
            if not result:
                if shell_ex:
                    raise shell_ex  # re-raise the underlying exception rather than making up our own.
                raise NoResultAfterInvocationError(memo_uri)
            return unwrap_remote_result(result)

    return run_shell_via_pickles_
