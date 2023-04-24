from thds.adls import ADLS_SCHEME, AdlsFqn

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
