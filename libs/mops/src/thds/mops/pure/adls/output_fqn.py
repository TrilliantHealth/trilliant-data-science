from thds.adls import AdlsFqn

from ..core import uris
from ..core.output_naming import invocation_output_uri


def invocation_output_fqn(storage_root: uris.UriIsh = "", name: str = "") -> AdlsFqn:
    """If your function only outputs a single blob to ADLS, you can safely
    use this without providing a name.  However, if you have multiple outputs
    from the same invocation, you must provide a meaningful name for each one.

    As an example:

    <pipeline> <function mod/name  > <your name     > <args,kwargs hash                                   >
    nppes/2023/thds.nppes.intake:run/<name goes here>/CoastOilAsset.IVZ9KplQKlNgxQHav0jIMUS9p4Kbn3N481e0Uvs
    """
    return AdlsFqn.parse(invocation_output_uri(storage_root, name=name))
