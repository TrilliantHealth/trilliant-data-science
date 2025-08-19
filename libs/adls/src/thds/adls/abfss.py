"""Translate ADLS URIs to ABFSS URIs (for use with Spark/Hadoop)."""
from .fqn import AdlsFqn

ABFSS_SCHEME = "abfss://"


class NotAbfssUri(ValueError):
    pass


def from_adls_fqn(fqn: AdlsFqn) -> str:
    return f"{ABFSS_SCHEME}{fqn.container}@{fqn.sa}.dfs.core.windows.net/{fqn.path.lstrip('/')}"


def from_adls_uri(uri: str) -> str:
    return from_adls_fqn(AdlsFqn.parse(uri))


def to_adls_fqn(abfss_uri: str) -> AdlsFqn:
    if not abfss_uri.startswith(ABFSS_SCHEME):
        raise NotAbfssUri(f"URI does not start with {ABFSS_SCHEME!r}: {abfss_uri!r}")
    container, rest = abfss_uri[len(ABFSS_SCHEME) :].split("@", 1)
    sa, path = rest.split(".dfs.core.windows.net/")
    return AdlsFqn.of(sa, container, path)
