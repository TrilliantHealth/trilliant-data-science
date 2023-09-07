## utilities for providing remote context for naming things uniquely:
import typing as ty

from thds.core.stack_context import StackContext

PipelineFunctionUniqueKey = StackContext("Mops2PipelineFunctionUniqueKey", default="")
FunctionArgumentsHashUniqueKey = StackContext("Mops2FunctionArgumentsHashUniqueKey", default="")


def pipeline_function_invocation_unique_key() -> ty.Optional[ty.Tuple[str, str]]:
    """A runner may provide a value for the underlying components, and
    if it does, the first string is required to be unique across all
    _separate_ functions running within a given pipeline id, and the
    second string is required to be unique for every unique invocation
    of that same function.

    If your code is _not_ running inside a mops runner, or
    the mops runner does not provide a value for this, you will
    instead get None.
    """
    pfi_key = PipelineFunctionUniqueKey(), FunctionArgumentsHashUniqueKey()
    if "" in pfi_key:  # if either of the elements was not supplied, we don't have anything!
        return None
    return pfi_key
