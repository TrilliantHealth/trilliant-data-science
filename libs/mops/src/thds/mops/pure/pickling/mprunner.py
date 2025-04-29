"""Provides concrete serialization/deserialization (via pickling) for the basic memoizing runner algorithm.

Contains default config, core 'state' and some rarely-used customization interfaces.

See runner.local.py for the core runner implementation.
"""

import typing as ty
from collections import defaultdict
from functools import partial

from typing_extensions import Self

from thds.core import cache, log
from thds.core.stack_context import StackContext

from ..._utils.once import Once
from ..core import memo, uris
from ..core.serialize_big_objs import ByIdRegistry, ByIdSerializer
from ..core.serialize_paths import CoordinatingPathSerializer
from ..core.types import Args, F, Kwargs, Serializer, T
from ..runner import local, shim_builder
from ..runner.types import Shim, ShimBuilder
from ..tools.summarize import run_summary
from . import _pickle, pickles, sha256_b64

RUNNER_NAME = "mops2-mpf"
Redirect = ty.Callable[[F, Args, Kwargs], F]
NO_REDIRECT = lambda f, _args, _kwargs: f  # noqa: E731
_ARGS_CONTEXT = StackContext[ty.Sequence]("args_kwargs", tuple())
_KWARGS_CONTEXT = StackContext[ty.Mapping]("args_kwargs", dict())
logger = log.getLogger(__name__)


def mp_shim(base_shim: Shim, shim_args: ty.Sequence[str]) -> ty.Any:
    return base_shim((RUNNER_NAME, *shim_args))


def _runner_prefix_for_pickled_functions(storage_root: str) -> str:
    return uris.lookup_blob_store(storage_root).join(storage_root, RUNNER_NAME)


class MemoizingPicklingRunner:
    """
    Runs callables in a process as defined by the Shim.
    This is often a remote process, however a local shim may be provided.
    """

    def __init__(
        self,
        shim: ty.Union[ShimBuilder, Shim],
        blob_storage_root: uris.UriResolvable,
        *,
        rerun_exceptions: bool = True,
        serialization_registry: ByIdRegistry[ty.Any, Serializer] = ByIdRegistry(),  # noqa: B008
        redirect: Redirect = NO_REDIRECT,
    ):
        """Construct a memoizing shim runner.

        Transmitted Path resources will be content-hash-addressed
        below the runner_prefix to save storage and increase chances
        of memoization. Named objects will be treated
        similarly. Function invocations will be pickled and stored
        under the current pipeline id since we do not have a way of
        inferring whether their associated code is safely content-addressable
        across runs.

        The Shim must forward control in the remote environment to a
        wrapper that will pull the function and arguments from the URI(s).

        A ShimBuilder will receive the original function and its
        original arguments, which you can use to determine which
        concrete Shim implementation to return for the given function
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
        self._shim_builder = shim_builder.make_builder(shim)
        self._get_storage_root = uris.to_lazy_uri(blob_storage_root)
        self._rerun_exceptions = rerun_exceptions
        self._by_id_registry = serialization_registry
        self._redirect = redirect

        self._run_directory = run_summary.create_mops_run_directory()

        self._calls_registry: dict[ty.Callable, list[ty.Callable]] = defaultdict(list)

    def calls(self, caller: ty.Callable, *callees: ty.Callable) -> Self:
        """Register that the first Callable calls the provided Callables(s).

        This is (currently) used to ensure that function-logic-keys on the callees affect
        the memoization of the caller. Callees that do not have a function-logic-key will
        be ignored for this purpose; however there are no known reasons why your
        underlying Callable should not have a function-logic-key, unless it has never been
        modified since its creation.

        The interface is more general and could in theory be used for other purposes in
        the future.
        """
        self._calls_registry[caller].extend(callees)
        return self  # returns self mainly to faciliate use with use_runner.

    def shared(self, *objs: ty.Any, **named_objs: ty.Any) -> None:
        """Set up memoizing pickle serialization for these objects.

        Provided names are used for debugging purposes only.
        """
        for obj in objs:
            self._by_id_registry[obj] = sha256_b64.Sha256B64Pickler()
        for name, obj in named_objs.items():
            self._by_id_registry[obj] = sha256_b64.Sha256B64Pickler(name)

    @cache.locking
    def _get_stateful_dumper(self, _root: str) -> _pickle.Dumper:
        """We want one of these per blob storage root, because the
        invocation and result must exist on the same blob store as
        any other automatically dumped objects, e.g. Paths or named
        objects, such that the full invocation payload is
        byte-for-byte identical, since its hash is our memoization
        key.
        """
        return _pickle.Dumper(
            ByIdSerializer(self._by_id_registry),
            CoordinatingPathSerializer(sha256_b64.Sha256B64PathStream(), Once()),
            _pickle.SourceArgumentPickler(),
            _pickle.NestedFunctionWithLogicKeyPickler(),
        )

    def _serialize_args_kwargs(
        self, storage_root: str, func: ty.Callable[..., T], args: Args, kwargs: Kwargs
    ) -> bytes:
        # Why do we need func in order to serialize args and kwargs? Because
        # we use it to bind the arguments to the function first, which makes that part
        # deterministic and also 'reifies' any default arguments, so we don't have any implicit state.
        return _pickle.freeze_args_kwargs(self._get_stateful_dumper(storage_root), func, args, kwargs)

    def _serialize_invocation(
        self, storage_root: str, func: ty.Callable[..., T], args_kwargs: bytes
    ) -> bytes:
        return _pickle.gimme_bytes(
            self._get_stateful_dumper(storage_root),
            pickles.Invocation(
                _pickle.wrap_f(self._redirect(func, _ARGS_CONTEXT(), _KWARGS_CONTEXT())),
                args_kwargs,
            ),
        )

    def _wrap_shim_builder(self, func: F, args: Args, kwargs: Kwargs) -> Shim:
        base_shim = self._shim_builder(func, args, kwargs)
        return partial(mp_shim, base_shim)

    def __call__(self, func: ty.Callable[..., T], args: Args, kwargs: Kwargs) -> T:
        """Return result of running this function remotely via the shim.

        Passes data to shim process via pickles in a Blob Store.

        May return cached (previously-computed) results found via the
        derived function memo URI, which contains the determinstic
        hashed bytes of all the function arguments, but also
        additional namespacing including pipeline_id as documented
        in memo.function_memospace.py.
        """
        logger.debug("Preparing to run function via remote shim")
        with _ARGS_CONTEXT.set(args), _KWARGS_CONTEXT.set(kwargs):
            return local.invoke_via_shim_or_return_memoized(
                self._serialize_args_kwargs,
                self._serialize_invocation,
                self._wrap_shim_builder,
                _pickle.read_metadata_and_object,
                self._run_directory,
                self._calls_registry,
            )(
                self._rerun_exceptions,
                memo.make_function_memospace(
                    _runner_prefix_for_pickled_functions(self._get_storage_root()), func
                ),
                func,
                args,
                kwargs,
            )
