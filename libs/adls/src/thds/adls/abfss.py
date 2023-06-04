"""Translate ADLS URIs to ABFSS URIs (for use with Spark/Hadoop)."""
from .fqn import AdlsFqn

_SCHEME = "abfss://"


def from_adls_fqn(fqn: AdlsFqn) -> str:
    return f"{_SCHEME}{fqn.container}@{fqn.sa}.dfs.core.windows.net/{fqn.path.lstrip('/')}"


def from_adls_uri(uri: str) -> str:
    return from_adls_fqn(AdlsFqn.parse(uri))


def to_adls_fqn(abfss_uri: str) -> AdlsFqn:
    assert abfss_uri.startswith(_SCHEME)
    container, rest = abfss_uri[len(_SCHEME) :].split("@", 1)
    sa, path = rest.split(".dfs.core.windows.net/")
    return AdlsFqn.of(sa, container, path)
