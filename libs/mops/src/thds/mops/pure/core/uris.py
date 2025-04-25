import io
import typing as ty
from pathlib import Path
from typing import Callable, Union

from thds.adls import AdlsFqn, AdlsRoot
from thds.core.stack_context import StackContext
from thds.mops._compat import importlib_metadata

from ..adls.blob_store import get_adls_blob_store
from .file_blob_store import get_file_blob_store
from .types import BlobStore

GetBlobStoreForUri = ty.Callable[[str], ty.Optional[BlobStore]]


# we add the ADLS blob store and FileBlobStore here because they are the 'blessed'
# implementations that our internal users rely on.
# Others can be registered via entry-points.
_REGISTERED_BLOB_STORES: ty.List[GetBlobStoreForUri] = [
    get_file_blob_store,
    get_adls_blob_store,
]


def register_blob_store(get_store: GetBlobStoreForUri) -> None:
    """Dynamically register a BlobStore implementation."""
    _REGISTERED_BLOB_STORES.append(get_store)


def load_plugin_blobstores() -> None:
    for entry_point in importlib_metadata.entry_points(group="thds.mops.pure.blob_stores"):
        try:
            register_blob_store(entry_point.load())
        except Exception as e:
            print(f"Error loading entry point {entry_point.name}: {e}")


def lookup_blob_store(uri: str) -> BlobStore:
    for get_store in _REGISTERED_BLOB_STORES[::-1]:
        if store := get_store(uri):
            return store
    raise ValueError(f"Unsupported URI: {uri}")


def get_root(uri: str) -> str:
    blob_store = lookup_blob_store(uri)
    return blob_store.control_root(uri)


UriIsh = Union[AdlsRoot, AdlsFqn, str, Path]
UriResolvable = Union[UriIsh, Callable[[], UriIsh]]


def to_lazy_uri(resolvable: UriResolvable) -> Callable[[], str]:
    if isinstance(resolvable, Path):
        return lambda: str(resolvable.resolve())
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
