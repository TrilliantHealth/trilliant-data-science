"""Utilities that may be useful for a remote entry implementation for your Runner.

None of them are required and may not be suitable for a given Runner implementation.
"""

import typing as ty

from thds.core import log, scope

from .. import deferred_work
from ..output_naming import FunctionArgumentsHashUniqueKey, PipelineFunctionUniqueKey

T_contra = ty.TypeVar("T_contra", contravariant=True)


class ResultChannel(ty.Protocol[T_contra]):
    """After remote invocation, respond with result.

    A remote invocation can succeed with a result or fail with an exception.
    """

    def return_value(self, __return_value: T_contra) -> None:
        ...  # pragma: no cover

    def exception(self, __ex: Exception) -> None:
        ...  # pragma: no cover


logger = log.getLogger(__name__)
_routing_scope = scope.Scope()


@_routing_scope.bound
def route_return_value_or_exception(
    channel: ResultChannel[T_contra],
    do_work_return_value: ty.Callable[[], T_contra],
    pipeline_id: str = "",
    pipeline_function_and_arguments_unique_key: ty.Optional[ty.Tuple[str, str]] = None,
) -> None:
    """The remote side of your runner implementation doesn't have to use this, but it's a reasonable approach."""
    _routing_scope.enter(deferred_work.push_non_context())
    # deferred work can be requested during result serialization, but because we don't want
    # to leave a 'broken' result payload (one that refers to unperformed deferred work,
    # maybe because of network or other failure), we simply don't open a deferred work
    # context on the remote side, which forces all the work to be performed as it is
    # added for deferral instead of actually being deferred.
    #
    # pushing this non-context is only necessary in the case of a thread-local
    # 'remote' invocation - in all true remote invocations, there will be no context open.

    _routing_scope.enter(log.logger_context(remote=pipeline_id))
    if pipeline_function_and_arguments_unique_key:
        pf_key, args_key = pipeline_function_and_arguments_unique_key
        _routing_scope.enter(PipelineFunctionUniqueKey.set(pf_key))
        _routing_scope.enter(FunctionArgumentsHashUniqueKey.set(args_key))
    try:
        # i want to _only_ run the user's function inside this try-catch.
        # If mops itself has a bug, we should not be recording that as
        # though it were an exception in the user's code.
        return_value = do_work_return_value()
    except Exception as ex:
        logger.exception("Failure to run remote function. Transmitting exception...")
        channel.exception(ex)
    else:
        logger.debug("Success running function remotely. Transmitting return value...")
        channel.return_value(return_value)
