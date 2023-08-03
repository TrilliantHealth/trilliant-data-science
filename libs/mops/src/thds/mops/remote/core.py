"""What lives here is the most general code that sketches out the basic concept.

Full implementations require a ser/de approach as well as an accessible storage location.
"""
import contextlib
import os
import typing as ty
from functools import wraps

from thds.core import log, scope
from thds.core.stack_context import StackContext

from ._root import _IS_CALLED_BY_RUNNER, get_pipeline_id, is_called_by_runner, set_pipeline_id
from .types import ResultChannel, Runner

logger = log.getLogger(__name__)
F = ty.TypeVar("F", bound=ty.Callable)
LazyBool = ty.Callable[[], bool]
T = ty.TypeVar("T")
FT = ty.TypeVar("FT", bound=ty.Callable[..., T], contravariant=True)
T_contra = ty.TypeVar("T_contra", contravariant=True)


def pure_remote(
    runner: Runner,
    *,
    bypass_remote: ty.Union[bool, LazyBool] = False,
    allow_nested: bool = False,  # future versions will switch this default to True.
) -> ty.Callable[[F], F]:
    """Wrap a function that is pure with respect to its arguments and result.

    Run that function on the provided runner.

    The arguments must be able to be transmitted by the runner to the
    remote context and not refer to anything that will not be
    accessible in that context.
    """

    def _skip_runner() -> bool:
        if isinstance(bypass_remote, bool):
            return bypass_remote
        bypass = bypass_remote()
        assert isinstance(bypass, bool), bypass
        return bypass

    def deco(f: F) -> F:
        @scope.bound
        @wraps(f)
        def __pure_remote_wrap(*args, **kwargs):  # type: ignore
            if is_called_by_runner():
                scope.enter(log.logger_context(remote=get_pipeline_id()))
                logger.debug("Calling function directly...")
            if is_called_by_runner() or _skip_runner():
                with _IS_CALLED_BY_RUNNER.set(False) if allow_nested else contextlib.nullcontext():
                    return f(*args, **kwargs)

            logger.debug("Forwarding local function call to runner...")
            return runner(f, args, kwargs)

        return ty.cast(F, __pure_remote_wrap)

    return deco


class SerializableThunk(ty.Generic[FT]):
    """A thing to be serialized, deserialized in a different process, and then directly invoked.

    As a thunk, it contains all of its parameters/context. Its result
    should usually be serializable by the same process that it was
    serialized, but ultimately this is up to the Runner
    implementation.

    Ideally, the bytes of this thunk will be deterministic for the
    same function and arguments, which would allow for result caching.

    The remote shell must handle the result or any Exceptions raised
    somehow. See `route_result_or_exception` for a reasonable approach.

    Note that if the name of this class or the names of its attributes
    change, it will likely become incompatible with all previously
    serialized instances, particularly if the serialization process is
    `pickle`.
    """

    def __init__(
        self,
        f: ty.Callable,
        args: ty.Sequence,
        kwargs: ty.Mapping[str, ty.Any],
    ):
        """Instance is constructed on the caller side."""
        self.f = f
        self.args = args
        self.kwargs = kwargs

    def __str__(self) -> str:
        return str(self.f)

    def __call__(self):
        """Run the function on the callee side."""
        logger.info(f"Running remote function {self.f} for {get_pipeline_id()}")
        # args and kwargs may be beefy, so we allow them to be
        # garbage-collected as soon as the underlying function
        # does by removing our own reference.
        args = self.args
        kwargs = self.kwargs
        self.args = []
        self.kwargs = dict()
        return self.f(*args, **kwargs)


InvocationUniqueKey = StackContext("InvocationUniqueKey", default="")


def invocation_unique_key() -> ty.Optional[str]:
    """A runner may provide a value for this, and if it does, it's
    required to be unique across all invocations of all mops
    functions. If your code is _not_ running inside a mops runner, or
    the mops runner does not provide a value for this, you will
    instead get None.
    """
    return InvocationUniqueKey() or None


@scope.bound
def route_result_or_exception(
    channel: ResultChannel[T_contra],
    get_thunk: ty.Callable[[], ty.Callable[[], T_contra]],
    pipeline_id: str = "",
    invocation_unique_key: str = "",
):
    """Your shell implementation doesn't have to use this, but it's a reasonable approach."""
    set_pipeline_id(pipeline_id)
    scope.enter(log.logger_context(remote=pipeline_id, pid=os.getpid()))
    thunk = get_thunk()  # defer until debug info set up
    try:
        scope.enter(InvocationUniqueKey.set(invocation_unique_key))
        channel.result(thunk())
    except Exception as ex:
        logger.exception(f"Thunk {thunk} invocation failed")
        channel.exception(ex)
