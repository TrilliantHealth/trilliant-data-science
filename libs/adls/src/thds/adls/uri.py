import typing as ty

from . import abfss, dbfs, fqn

HTTPS_SCHEME = "https"


def resolve_uri(uri: str) -> ty.Optional[fqn.AdlsFqn]:
    if uri.startswith(fqn.ADLS_SCHEME):
        return fqn.parse(uri)
    elif uri.startswith(abfss.ABFSS_SCHEME):
        return abfss.to_adls_fqn(uri)
    elif uri.startswith(HTTPS_SCHEME):
        uri = uri.replace("https://", "adls://")
        uri = uri.replace(".blob.core.windows.net", ".dfs.core.windows.net")
        uri = uri.replace(".dfs.core.windows.net", "")
        return fqn.parse(uri)
    elif uri.startswith(dbfs.DBFS_SCHEME):
        return dbfs.to_adls_fqn(uri)
    return None


def resolve_any(fqn_or_uri: ty.Union[str, fqn.AdlsFqn]) -> ty.Optional[fqn.AdlsFqn]:
    return resolve_uri(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
