from typing import Callable, Union

from thds.adls import ADLS_SCHEME, AdlsFqn, AdlsRoot

from ._adls import AdlsFileSystem
from .types import BlobStore


def lookup_blob_store(uri: str) -> BlobStore:
    if uri.startswith(ADLS_SCHEME):
        return AdlsFileSystem()
    raise ValueError(f"Unsupported URI: {uri}")


def get_root(uri: str) -> str:
    if uri.startswith(ADLS_SCHEME):
        return str(AdlsFqn.parse(uri).root())
    raise ValueError(f"Unsupported URI: {uri}")


UriIsh = Union[AdlsRoot, AdlsFqn, str]
UriResolvable = Union[UriIsh, Callable[[], UriIsh]]


def to_lazy_uri(resolvable: UriResolvable) -> Callable[[], str]:
    if isinstance(resolvable, (str, AdlsRoot, AdlsFqn)):
        return lambda: str(resolvable)
    if callable(resolvable):
        return lambda: str(resolvable())  # type: ignore
    raise TypeError(type(resolvable))
