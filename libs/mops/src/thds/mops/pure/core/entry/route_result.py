"""Utilities that may be useful for a remote entry implementation for your Runner.

None of them are required and may not be suitable for a given Runner implementation.
"""

import typing as ty

from thds.core import log, scope

from ..output_naming import FunctionArgumentsHashUniqueKey, PipelineFunctionUniqueKey
from ..pipeline_id import set_pipeline_id

logger = log.getLogger(__name__)
T_contra = ty.TypeVar("T_contra", contravariant=True)


class ResultChannel(ty.Protocol[T_contra]):
    """After remote invocation, respond with result.

    A remote invocation can succeed with a result or fail with an exception.
    """

    def result(self, __result: T_contra) -> None:
        ...  # pragma: no cover

    def exception(self, __ex: Exception) -> None:
        ...  # pragma: no cover


@scope.bound
def route_result_or_exception(
    channel: ResultChannel[T_contra],
    do_work_return_result_thunk: ty.Callable[[], T_contra],
    pipeline_id: str = "",
    pipeline_function_and_arguments_unique_key: ty.Optional[ty.Tuple[str, str]] = None,
):
    """The remote side of your runner implementation doesn't have to use this, but it's a reasonable approach."""
    set_pipeline_id(pipeline_id)
    scope.enter(log.logger_context(remote=pipeline_id))
    if pipeline_function_and_arguments_unique_key:
        pf_key, args_key = pipeline_function_and_arguments_unique_key
        scope.enter(PipelineFunctionUniqueKey.set(pf_key))
        scope.enter(FunctionArgumentsHashUniqueKey.set(args_key))
    try:
        # i want to _only_ run the user's function inside this try-catch.
        # If mops itself has a bug, we should not be recording that as
        # though it were an exception in the user's code.
        result = do_work_return_result_thunk()
    except Exception as ex:
        logger.exception("Failure to run thunk. Transmitting exception...")
        channel.exception(ex)
    else:
        channel.result(result)
