"""Joins pickle functionality and ADLS functionality into a full
implementation for the pure_remote Channel/Runner system.
"""
import hashlib
import typing as ty

from cachetools import LRUCache
from typing_extensions import Protocol

from thds.adls import AdlsFqn
from thds.core import scope
from thds.core.log import getLogger

from ..__about__ import backward_compatible_with
from ..config import adls_remote_tmp_container, adls_remote_tmp_sa
from ..locked_cache import locked_cached
from ._adls import AdlsFileSystem, AdlsFileSystemClient, join, yield_filenames
from ._adls_serde import (
    ADLS_CONTEXT,
    AdlsContext,
    Dumper,
    NamedAdlsPickler,
    make_dumper,
    make_read_object,
)
from ._byos import BYOS
from ._once import Once
from ._pickle import gimme_bytes, wrap_f
from ._registry import MAIN_HANDLER_BASE_ARGS, register_main_handler
from .core import SerializableThunk, forwarding_call, get_pipeline_id
from .remote_file import trigger_dest_files_download, trigger_src_files_upload
from .types import T

logger = getLogger(__name__)


Shell = ty.Callable[[ty.Sequence[str]], ty.Any]
"""A Shell is a way of getting back into a Python process with enough
context to download the function and its arguments from the ADLS
location where AdlsPickleRunner placed it, and then invoke the
function. All arguments are strings because it is assumed that this
represents some kind of command line invocation.

The Shell must be a blocking call, and its result(s) must be available
immediately after its return.
"""


class _ShellBuilder(Protocol):
    def __call__(self, __f, *__args, **__kwargs) -> Shell:
        ...


class ShellBuilder(ty.NamedTuple):
    """You can also dynamically build your Shell based on the function and arguments passed.

    This allows sharing the core AdlsPickleRunner state/context between subtly different calls.
    """

    shell_builder: _ShellBuilder


class NoResultAfterInvocationError(Exception):
    """Raised if the remotely-invoked function does not provide any result."""


def _mk_raiser(callable: ty.Callable[[], Exception]):
    def _raiser_():
        raise callable()

    return _raiser_


def _invocation_path(call_id: str) -> str:
    return call_id + "/invocation"


def _result_path(call_id: str) -> str:
    return call_id + "/result"


def _exception_path(call_id: str) -> str:
    return call_id + "/exception"


def _make_once(_pipeline_id: str) -> Once:
    return Once()


def _make_byos(_pipeline_id: str) -> BYOS:
    return BYOS()


def _make_pipeline_init_file_exists_cache(
    sa: str, container: str, adls_prefix: str
) -> ty.Callable[[str], bool]:
    """At the beginning of a function run, we don't expect most of
    these files to exist - they will be the results of our upcoming
    run. Therefore we err on the side of using a cache to tell us that they do not exist.
    """
    adls_prefix = adls_prefix.lstrip("/")
    logger.info(f"Searching {adls_prefix} for previous runs")
    known_filenames = set(yield_filenames(AdlsFileSystemClient(sa, container), adls_prefix))
    if len(known_filenames):
        logger.info(
            f"Found {len(known_filenames)} at the pipeline prefix {adls_prefix} "
            "- these may save some time."
        )

    def file_exists(full_filename: str) -> bool:
        full_filename = full_filename.lstrip("/")  # ADLS returns paths with no forward slash
        if full_filename.startswith(adls_prefix):
            return full_filename in known_filenames
        logger.warning(f"Having to search elsewhere for {full_filename}")
        return AdlsFileSystem(sa, container).file_exists(full_filename)

    return file_exists


def _make_name(f):
    try:
        return f"{f.__module__}:{f.__name__}"
    except AttributeError:
        return f.__class__.__name__


def _default_adls_root() -> AdlsFqn:
    return AdlsFqn(
        adls_remote_tmp_sa(),
        adls_remote_tmp_container(),
        f"/mops-{backward_compatible_with()}-adls-pickle-runner",
    )


class AdlsPickleRunner:
    """Runs functions in a remote process as specified by the Shell."""

    def __init__(
        self,
        shell: ty.Union[Shell, ShellBuilder],
        adls_path: ty.Optional[AdlsFqn] = None,
    ):
        """Construct a repeatable shell runner with a shared pipeline_id.

        Transmitted Path resources will be reused across multiple runs
        with this same Runner, based on prefix created by `prefix +
        pipeline_id`.  By default, pipeline_id will be auto-generated,
        but if you have a known, reproducible run, you should set this
        so that your data can get reused.

        Originally, a concrete shell was required. This has been made
        more flexible; you may now provide a Shell Builder, which will
        receive the original function and its original arguments,
        which you can use to determine which concrete Shell
        implementation to return for the given function call. The
        Shell must still support pulling the function and arguments
        from ADLS as usual.
        """
        if isinstance(shell, ShellBuilder):
            # this is for backward compatibility. arguably we should simplify this interface.
            self.shell_builder = ty.cast(_ShellBuilder, shell.shell_builder)
        else:
            self.shell_builder = lambda *_args, **_kws: shell

        self._adls_loc = adls_path or _default_adls_root()
        # synchronization per pipeline id:
        self._pipeline_once = locked_cached(LRUCache(1))(_make_once)
        self._pipeline_byos = locked_cached(LRUCache(1))(_make_byos)
        self._pre_run_file_exists = locked_cached(LRUCache(20))(_make_pipeline_init_file_exists_cache)

    def named(self, **objs: ty.Any):
        """Set up shared pickle serialization for these objects.

        Each name must be unique within the entire pipeline.
        """
        for name, obj in objs.items():
            self._pipeline_byos(get_pipeline_id()).byos(obj, NamedAdlsPickler(name))

    @scope.bound
    def __call__(self, f: ty.Callable[..., T], args: ty.Sequence, kwargs: ty.Mapping[str, ty.Any]) -> T:
        """Return result of running this function remotely via the shell.

        Passes data to shell process via pickles in ADLS.

        May return cached (previously-computed) results based on the
        deterministic hashed bytes of all of the arguments, _plus_ the
        pipeline ID.
        """
        trigger_src_files_upload(args, kwargs)

        pipeline_id = get_pipeline_id()
        sa, container, path = self._adls_loc
        pipeline_dir = join(path, pipeline_id)
        logger.debug("Preparing to run function via remote shell")

        fs = AdlsFileSystem(sa, container)
        scope.enter(ADLS_CONTEXT.set(AdlsContext(fs, pipeline_dir)))
        dumper = make_dumper(
            fs,
            pipeline_dir,
            self._pipeline_once(pipeline_id),
            self._pipeline_byos(pipeline_id),
        )

        invoc_bytes = gimme_bytes(
            dumper,
            SerializableThunk(
                wrap_f(f),
                args,
                kwargs,
            ),
        )
        # deterministic call_id based on the hashed bytes of the invocation itself
        function_dir = join(pipeline_dir, _make_name(f))
        call_id = join(
            function_dir,
            hashlib.sha256(invoc_bytes).hexdigest(),
        )
        read_object = make_read_object(fs)

        def check_result(file_exists: ty.Callable[[str], bool]) -> ty.Optional[ty.Callable[[], T]]:
            result_path = _result_path(call_id)
            if file_exists(result_path):
                return lambda: trigger_dest_files_download(ty.cast(T, read_object(result_path)))
            exc_path = _exception_path(call_id)
            if file_exists(exc_path):
                return _mk_raiser(lambda: ty.cast(Exception, read_object(exc_path)))
            return None

        # it's possible that our result may already exist from a previous run of this pipeline id.
        # we can short-circuit the entire process by looking for that result and returning it immediately.
        give_result = check_result(self._pre_run_file_exists(sa, container, function_dir))
        if give_result:
            logger.info(f"Result for {call_id} already exists and is being returned without invocation!")
            return give_result()
        # but if it does not exist, we need to run.

        fs.put_bytes(_invocation_path(call_id), invoc_bytes, type_hint="invocation")
        try:
            shell_ex = None
            self.shell_builder(f, *args, **kwargs)(
                [
                    *MAIN_HANDLER_BASE_ARGS,
                    AdlsPickleRunner.__class__.__name__,
                    sa,
                    container,
                    pipeline_dir,
                    call_id,
                    pipeline_id,
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

        give_result = check_result(fs.file_exists)
        if not give_result:
            if shell_ex:
                raise shell_ex  # re-raise the underlying exception rather than making up our own.
            raise NoResultAfterInvocationError(call_id)
        return give_result()


class _ResultChannel:
    def __init__(self, fs: AdlsFileSystem, dumper: Dumper, call_id: str):
        self.fs = fs
        self.dumper = dumper
        self.call_id = call_id

    def result(self, result: T):
        self.fs.put_bytes(
            _result_path(self.call_id),
            gimme_bytes(self.dumper, result),
            type_hint="result",
        )

    def exception(self, exception: Exception):
        self.fs.put_bytes(
            _exception_path(self.call_id),
            gimme_bytes(self.dumper, exception),
            type_hint="EXCEPTION",
        )


def run_adls_pickle_invocation(*shell_args: str):
    """Call directly from the command line. The arguments are those supplied by AdlsPickleRunner.

    python -m thds.mops.remote.main run_adls_pickle_invocation \
        sa container pipeline_dir call_id pipeline_id
    """
    sa, container, pipeline_dir, call_id, pipeline_id = shell_args
    fs = AdlsFileSystem(sa, container)
    forwarding_call(
        _ResultChannel(fs, make_dumper(fs, pipeline_dir, Once(), BYOS()), call_id),
        ty.cast(
            SerializableThunk,
            make_read_object(fs)(_invocation_path(call_id), type_hint="invocation"),
        ),
        pipeline_id,
    )


register_main_handler(AdlsPickleRunner.__class__.__name__, run_adls_pickle_invocation)
