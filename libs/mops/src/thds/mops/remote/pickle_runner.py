"""Joins pickle functionality and ADLS functionality

into a full implementation for the pure_remote Channel/Runner system.
"""
import threading
import typing as ty
from functools import lru_cache

from thds.core import scope
from thds.core.log import getLogger

from ..__about__ import backward_compatible_with
from ..colorize import colorized
from ..config import adls_max_clients
from ..exception import catch
from ._byos import ByIdRegistry, MemoizingSerializer
from ._content_aware_uri_serde import STORAGE_ROOT, SharedPickler, make_dumper, make_read_object
from ._memoize import args_kwargs_content_address, get_mask_or_pipeline_id, make_function_memospace
from ._once import Once
from ._paths import PathContentAddresser
from ._pickle import Dumper, Serializer, freeze_args_kwargs, gimme_bytes, unfreeze_args_kwargs, wrap_f
from ._registry import MAIN_HANDLER_BASE_ARGS, register_main_handler
from ._uris import get_root, lookup_blob_store
from .core import SerializableThunk, forwarding_call
from .remote_file import trigger_dest_files_placeholder_write, trigger_src_files_upload
from .types import Args, BlobStore, Kwargs, NoResultAfterInvocationError, Shell, T, _ShellBuilder

logger = getLogger(__name__)


_RUNNER_SUFFIX = f"mops/pipeline-pickled-functions-v{backward_compatible_with()}"


def _runner_prefix_for_pickled_functions(storage_root: str) -> str:
    return lookup_blob_store(storage_root).join(storage_root, _RUNNER_SUFFIX)


def _extract_invocation_unique_key(memo_uri: str) -> str:
    runner_loc = memo_uri.find(_RUNNER_SUFFIX)
    if runner_loc >= 0:
        return memo_uri[runner_loc + len(_RUNNER_SUFFIX) :].lstrip("/")
    storage_root = get_root(memo_uri)
    return memo_uri[len(storage_root) :]


class MemoizingPickledFunctionRunner:
    """Runs functions in a remote process as defined by the Shell."""

    def __init__(
        self,
        shell_builder: _ShellBuilder,
        storage_root: str,
        *,
        rerun_exceptions: bool = True,
        serialization_registry: ByIdRegistry[ty.Any, Serializer] = ByIdRegistry(),  # noqa: B008
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

        The ShellBuilder will receive the original function and its
        original arguments, which you can use to determine which
        concrete Shell implementation to return for the given function
        call.

        `rerun_exceptions` will cause a pre-existing `exception`
        result to be ignored, as though Exceptions in your function
        are the result of transient errors and not an expected return
        value of a (simulated) pure function.

        """
        self._shell_builder = shell_builder
        self._runner_prefix = _runner_prefix_for_pickled_functions(storage_root)
        self._rerun_exceptions = rerun_exceptions
        self._by_id_registry = serialization_registry

    def named(self, **objs: ty.Any):
        """DEPRECATED; use `shared` instead.

        Set up memoizing pickle serialization for these objects.

        The names are used for debugging purposes only.
        """

    def shared(self, *objs: ty.Any, **named_objs: ty.Any):
        """Set up memoizing pickle serialization for these objects.

        Provided names are used for debugging purposes only.
        """
        for obj in objs:
            self._by_id_registry[obj] = SharedPickler()
        for name, obj in named_objs.items():
            self._by_id_registry[obj] = SharedPickler(name)

    # still not totally sure about thread-safety of lru_cache
    @lru_cache(maxsize=None)  # noqa: B019
    def _get_dumper(self, _root: str) -> Dumper:
        """We want one of these per ADLS SA/container, because the
        invocation and result must exist on the same SA/container as
        any other automatically dumped objects, e.g. Paths or named
        objects, such that the full invocation payload is
        byte-for-byte identical, since its hash is our memoization
        key.
        """
        return make_dumper(Once(), MemoizingSerializer(self._by_id_registry), PathContentAddresser())

    def __call__(self, f: ty.Callable[..., T], args: Args, kwargs: Kwargs) -> T:
        """Return result of running this function remotely via the shell.

        Passes data to shell process via pickles in ADLS.

        May return cached (previously-computed) results found via the
        derived function memo URI, which contains the determinstic
        hashed bytes of all the function arguments, but also
        additional namespacing including pipeline_id as documented
        in _memoize.py.
        """
        logger.debug("Preparing to run function via remote shell")
        return _pickle_func_and_run_via_shell(
            make_function_memospace(self._runner_prefix, f),
            self._get_dumper,
            f,
        )(self._shell_builder(f, *args, **kwargs), self._rerun_exceptions, args, kwargs)


# these two semaphores allow us to prioritize getting meaningful units
# of progress _complete_, rather than issuing many instructions to the
# underlying client and allowing it to randomly order the operations
# such that it takes longer to get a full unit of work complete.
_OUT_SEMAPHORE = threading.BoundedSemaphore(int(adls_max_clients()))
# _OUT prioritizes uploading a single invocation and its dependencies so the Shell can start running.
_IN_SEMAPHORE = threading.BoundedSemaphore(int(adls_max_clients()))
# _IN prioritizes retrieving the result of a Shell that has completed.

_INVOCATION = "invocation"
_RESULT = "result"
_EXCEPTION = "exception"
_DarkBlue = colorized(fg="white", bg="#00008b")
_LogKnownResult = lambda s: logger.info(_DarkBlue(s))  # noqa: E731


class _NestedFunctionPickle(ty.NamedTuple):
    """By pickling args-kwargs on its own, we can get a hash of just those."""

    f: ty.Callable
    args_kwargs_pickle: bytes


def _pickle_func_and_run_via_shell(
    function_memospace: str,
    get_dumper: ty.Callable[[str], Dumper],
    f: ty.Callable[..., T],
) -> ty.Callable[[Shell, bool, Args, Kwargs], T]:
    storage_root = get_root(function_memospace)

    @scope.bound
    def run_shell_via_pickles_(
        shell: Shell,
        rerun_exceptions: bool,
        args: Args,
        kwargs: Kwargs,
    ) -> T:
        scope.enter(STORAGE_ROOT.set(storage_root))
        fs = lookup_blob_store(function_memospace)
        dumper = get_dumper(storage_root)

        # the network ops being grouped by _OUT include one or more
        # download attempts (consider possible pickled Paths) plus
        # one or more uploads (embedded Paths, invocation).
        with _OUT_SEMAPHORE:
            trigger_src_files_upload(args, kwargs)
            args_kwargs_bytes = freeze_args_kwargs(dumper, f, args, kwargs)
            memo_uri = fs.join(function_memospace, args_kwargs_content_address(args_kwargs_bytes))

            class _success(ty.NamedTuple):
                result: ty.Any

            class _error(ty.NamedTuple):
                exception: ty.Any

            read_result = make_read_object(_RESULT, _success)
            read_exception = make_read_object("EXCEPTION", _error)

            def check_result(
                rerun_excs: bool = False,
            ) -> ty.Union[None, _success, _error]:
                with catch(fs.is_blob_not_found):
                    return read_result(fs.join(memo_uri, _RESULT))
                if rerun_excs:
                    return None
                with catch(fs.is_blob_not_found):
                    return read_exception(fs.join(memo_uri, _EXCEPTION))
                return None

            def give_result(result: ty.Union[_success, _error]) -> T:
                if isinstance(result, _success):
                    trigger_dest_files_placeholder_write(result.result)
                    return ty.cast(T, result.result)
                assert isinstance(result, _error), "Must be _error or _success"
                raise result.exception

            # it's possible that our result may already exist from a previous run of this pipeline id.
            # we can short-circuit the entire process by looking for that result and returning it immediately.
            result = check_result(rerun_excs=rerun_exceptions)
            if result:
                _LogKnownResult(
                    f"Result for {memo_uri} already exists and is being returned without invocation!"
                )
                return give_result(result)
            # but if it does not exist, we need to upload the invocation and then run the shell.
            fs.put(
                fs.join(memo_uri, _INVOCATION),
                gimme_bytes(
                    dumper,
                    _NestedFunctionPickle(wrap_f(f), args_kwargs_bytes),
                ),
                type_hint=_INVOCATION,
            )

        try:
            shell_ex = None
            shell(
                [
                    *MAIN_HANDLER_BASE_ARGS,
                    MemoizingPickledFunctionRunner.__name__,
                    memo_uri,
                    get_mask_or_pipeline_id(),  # for debugging only
                ]
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
            result = check_result()
            if not result:
                if shell_ex:
                    raise shell_ex  # re-raise the underlying exception rather than making up our own.
                raise NoResultAfterInvocationError(memo_uri)
            return give_result(result)

    return run_shell_via_pickles_


class _ResultExcChannel(ty.NamedTuple):
    fs: BlobStore
    dumper: Dumper
    call_id: str

    def result(self, r: T):
        self.fs.put(
            self.fs.join(self.call_id, _RESULT),
            gimme_bytes(self.dumper, r),
            type_hint=_RESULT,
        )

    def exception(self, exc: Exception):
        self.fs.put(
            self.fs.join(self.call_id, _EXCEPTION),
            gimme_bytes(self.dumper, exc),
            type_hint="EXCEPTION",
        )


def run_pickled_invocation(*shell_args: str):
    """Call directly from the command line. The arguments are those supplied by PickleRunner.

    python -m thds.mops.remote.main run_pickle_invocation \
        memo_uri pipeline_id
    """
    memo_uri, pipeline_id = shell_args
    fs = lookup_blob_store(memo_uri)

    def unpickle_nested_invocation() -> SerializableThunk:
        nested = ty.cast(
            _NestedFunctionPickle,
            make_read_object(_INVOCATION)(fs.join(memo_uri, _INVOCATION)),
        )
        return SerializableThunk(nested.f, *unfreeze_args_kwargs(nested.args_kwargs_pickle))

    forwarding_call(
        _ResultExcChannel(
            fs,
            make_dumper(Once(), MemoizingSerializer(ByIdRegistry()), PathContentAddresser()),
            memo_uri,
        ),
        unpickle_nested_invocation,
        pipeline_id,
        _extract_invocation_unique_key(memo_uri),
    )


register_main_handler(MemoizingPickledFunctionRunner.__name__, run_pickled_invocation)
