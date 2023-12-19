from thds.adls import AdlsFqn

from ..core import types, uris
from ..core.output_naming import pipeline_function_invocation_unique_key


def invocation_output_fqn(storage_root: uris.UriIsh = "", name: str = "") -> AdlsFqn:
    """If your function only outputs a single blob to ADLS, you can safely
    use this without providing a name.  However, if you have multiple outputs
    from the same invocation, you must provide a meaningful name for each one.

    As an example:

    <pipeline> <function mod/name  > <your name     > <args,kwargs hash                                   >
    nppes/2023/thds.nppes.intake:run/<name goes here>/CoastOilAsset.IVZ9KplQKlNgxQHav0jIMUS9p4Kbn3N481e0Uvs
    """
    storage_root = storage_root or uris.ACTIVE_STORAGE_ROOT()
    pf_fa = pipeline_function_invocation_unique_key()
    if not pf_fa:
        raise types.NotARunnerContext(
            "`invocation_output_fqn` must be used in a `thds.mops.pure` runner context."
        )
    pipeline_function_key, function_arguments_key = pf_fa
    return (
        AdlsFqn.parse(str(storage_root))
        / pipeline_function_key
        / name
        / "--".join([function_arguments_key, name])
        # we use the name twice now, so that the final part of the path also has a file extension
    )
