import typing as ty

from . import abfss, dbfs, fqn

HTTPS_SCHEME = "https"

UriIsh = ty.Union[str, fqn.AdlsFqn]
# Could be an `AdlsFqn`, a supported URI scheme, or potentially any string.


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


def parse_uri(uri: str) -> fqn.AdlsFqn:
    """Strict/raises error if not a known ADLS URI"""
    if name := resolve_uri(uri):
        return name
    raise fqn.NotAdlsUri(uri)


def resolve_any(fqn_or_uri: UriIsh) -> ty.Optional[fqn.AdlsFqn]:
    return resolve_uri(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri


def parse_any(fqn_or_uri: UriIsh) -> fqn.AdlsFqn:
    return parse_uri(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
