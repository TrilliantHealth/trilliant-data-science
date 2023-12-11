import typing as ty

from .abfss import ABFSS_SCHEME
from .abfss import to_adls_fqn as abfss_to_fqn
from .dbfs import DBFS_SCHEME
from .dbfs import to_adls_fqn as dbfs_to_fqn
from .fqn import ADLS_SCHEME, AdlsFqn, parse

HTTPS_SCHEME = "https"


def resolve_uri(uri: str) -> ty.Optional[AdlsFqn]:
    if uri.startswith(ADLS_SCHEME):
        return parse(uri)
    elif uri.startswith(ABFSS_SCHEME):
        return abfss_to_fqn(uri)
    elif uri.startswith(HTTPS_SCHEME):
        uri = uri.replace("https://", "adls://")
        uri = uri.replace(".blob.core.windows.net", ".dfs.core.windows.net")
        uri = uri.replace(".dfs.core.windows.net", "")
        return parse(uri)
    elif uri.startswith(DBFS_SCHEME):
        return dbfs_to_fqn(uri)
    return None


def resolve_any(fqn_or_uri: ty.Union[str, AdlsFqn]) -> ty.Optional[AdlsFqn]:
    return resolve_uri(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
