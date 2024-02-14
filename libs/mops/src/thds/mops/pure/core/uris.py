import io
from pathlib import Path
from typing import Callable, Union

from thds.adls import ADLS_SCHEME, AdlsFqn, AdlsRoot
from thds.core.files import is_file_uri, to_uri
from thds.core.stack_context import StackContext

# we import the ADLS blob store here even though this is the only place in core where we 'touch'
# the ADLS implementation.
#
# In theory this could be abstracted via a registration process instead,
# but that seems like over-engineering at this point.
from ..adls.blob_store import get_store
from .file_blob_store import FileBlobStore
from .types import BlobStore


def lookup_blob_store(uri: str) -> BlobStore:
    if uri.startswith(ADLS_SCHEME):
        return get_store()
    if is_file_uri(uri):
        return FileBlobStore()
    raise ValueError(f"Unsupported URI: {uri}")


def get_root(uri: str) -> str:
    if uri.startswith(ADLS_SCHEME):
        return str(AdlsFqn.parse(uri).root())
    if is_file_uri(uri):
        return to_uri(Path.home())
    raise ValueError(f"Unsupported URI: {uri}")


UriIsh = Union[AdlsRoot, AdlsFqn, str]
UriResolvable = Union[UriIsh, Callable[[], UriIsh]]


def to_lazy_uri(resolvable: UriResolvable) -> Callable[[], str]:
    if isinstance(resolvable, (str, AdlsRoot, AdlsFqn)):
        return lambda: str(resolvable)
    if callable(resolvable):
        return lambda: str(resolvable())  # type: ignore
    raise TypeError(type(resolvable))


def get_bytes(remote_uri: str, type_hint: str) -> bytes:
    blob_store = lookup_blob_store(remote_uri)
    with io.BytesIO() as tb:
        blob_store.readbytesinto(remote_uri, tb, type_hint=type_hint)
        tb.seek(0)
        return tb.read()


# ACTIVE_STORAGE_ROOT is meant as a global, non-semantic URI prefix.
# In other words, it should have nothing to do with your application
ACTIVE_STORAGE_ROOT: StackContext[str] = StackContext("ACTIVE_STORAGE_ROOT", "")
# objects referencing this StackContext must be used in the same thread where they were created.


def active_storage_root() -> str:
    assert ACTIVE_STORAGE_ROOT(), "ACTIVE_STORAGE_ROOT must be set before use."
    return ACTIVE_STORAGE_ROOT()
