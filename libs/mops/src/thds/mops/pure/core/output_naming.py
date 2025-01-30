## utilities for providing remote context for naming things uniquely:
import typing as ty

from thds.core.stack_context import StackContext

from . import types, uris

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


def invocation_output_uri(storage_root: uris.UriIsh = "", name: str = "") -> str:
    """If your function only outputs a single blob, you can safely use this without
    providing a name.  However, if you have multiple outputs from the same invocation, you
    must provide a meaningful name for each one.

    As an example:

    <pipeline> <function mod/name  > <your name     > <args,kwargs hash                                   >
    nppes/2023/thds.nppes.intake:run/<name goes here>/CoastOilAsset.IVZ9KplQKlNgxQHav0jIMUS9p4Kbn3N481e0Uvs
    """
    storage_root = str(storage_root or uris.ACTIVE_STORAGE_ROOT())
    pf_fa = pipeline_function_invocation_unique_key()
    if not pf_fa:
        raise types.NotARunnerContext(
            "`invocation_output_uri` must be used in a `thds.mops.pure` runner context."
        )
    pipeline_function_key, function_arguments_key = pf_fa
    return uris.lookup_blob_store(storage_root).join(
        storage_root,
        pipeline_function_key,
        "--".join(filter(None, [name, function_arguments_key])),
        name,
        # we use the name twice now, so that the final part of the path also has a file extension
    )
