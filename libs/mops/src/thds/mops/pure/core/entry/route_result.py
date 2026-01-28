"""Utilities that may be useful for a remote entry implementation for your Runner.

None of them are required and may not be suitable for a given Runner implementation.
"""

import typing as ty

from thds.core import log, scope

from .. import deferred_work
from ..output_naming import uri_assignment_context

T_contra = ty.TypeVar("T_contra", contravariant=True)


class ResultChannel(ty.Protocol[T_contra]):
    """After remote invocation, respond with result.

    A remote invocation can succeed with a result or fail with an exception.
    """

    def return_value(self, __return_value: T_contra) -> None: ...  # pragma: no cover

    def exception(self, __ex: Exception) -> None: ...  # pragma: no cover


logger = log.getLogger(__name__)
_routing_scope = scope.Scope()


@_routing_scope.bound
def route_return_value_or_exception(
    channel: ResultChannel[T_contra],
    do_work_return_value: ty.Callable[[], T_contra],
    memo_uri: str,
    runner_prefix: str = "",  # this must be present in your memo URI
    invocation_run_id: str = "",
) -> None:
    """The remote side of your runner implementation doesn't have to use this, but it's a reasonable approach."""
    _routing_scope.enter(deferred_work.open_context())
    memo_uri_components = _routing_scope.enter(
        uri_assignment_context(memo_uri, runner_prefix, invocation_run_id)
    )
    _routing_scope.enter(log.logger_context(remote=memo_uri_components.pipeline_id))

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
