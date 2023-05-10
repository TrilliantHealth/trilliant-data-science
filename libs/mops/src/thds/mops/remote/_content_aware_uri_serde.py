"""Context-local, content-aware ser/de from/to a known URI prefix.

Basically, we take some generic pickle utilities and stitch them together into something
that efficiently serializes object graphs to a combination of locations at some URI prefix,
such that they are self-deserializing (via CallableUnpickler) on the other side.
"""
import hashlib
import io
import pickle
import typing as ty

from thds.core.hashing import hash_using
from thds.core.log import getLogger
from thds.core.stack_context import StackContext

from ..config import get_memo_storage_root
from ._byos import MemoizingSerializer
from ._hash import nest
from ._once import Once
from ._paths import Downloader, PathContentAddresser, PathPickler
from ._pickle import CallableUnpickler, Dumper
from ._uris import lookup_blob_store
from .types import BlobStore

logger = getLogger(__name__)
T = ty.TypeVar("T")


_CONTENT_HASH_ADDRESSED = "mops/content-hash-addressed"
# we can save on storage and simplify lots of internals if we just
# hash all blobs and upload them to a key that is their hash.


# STORAGE_ROOT is meant as a global, non-semantic URI prefix.
# In other words, it should have nothing to do with your application
STORAGE_ROOT: StackContext[str] = StackContext("STORAGE_ROOT", default="")
# We use StackContext in order to perform lazy dependency injection - it's possible
# that various configurations may arise requiring the root ADLS location to need to be changed
# for a specific function invocation, and this allows the configuration to take hold naturally
# without lots of additional bookkeeping.

# objects referencing this StackContext must be used in the same thread where they were created.


def _get_storage_root() -> str:
    return STORAGE_ROOT() or get_memo_storage_root()


def _content_addressed(key: str) -> str:
    return f"{_get_storage_root()}/{_CONTENT_HASH_ADDRESSED}/{key}"


class _downloader(ty.NamedTuple):
    """Needs to be picklable/serializable."""

    uri: str

    def __call__(self, byts: ty.IO[bytes]):
        return lookup_blob_store(self.uri).read(self.uri, byts)


class _ContentAddressedPathStream:
    def local_to_remote(self, src: ty.IO[bytes], key: str):
        """Return fully qualified remote information after put."""
        full_remote_key = _content_addressed(key)
        lookup_blob_store(full_remote_key).put(full_remote_key, src, type_hint="path")

    def get_downloader(self, remote_key: str) -> Downloader:
        return _downloader(_content_addressed(remote_key))  # type: ignore # NamedTuple silliness


def make_dumper(
    once: Once, memo_ser: MemoizingSerializer, path_addresser: PathContentAddresser
) -> Dumper:
    """Requires CONTAINER to be set before it is used."""
    return Dumper([PathPickler(_ContentAddressedPathStream(), once, path_addresser), memo_ser])


def _get_bytes(blob_store: BlobStore, remote_uri: str, type_hint: str) -> bytes:
    with io.BytesIO() as tb:
        blob_store.read(remote_uri, tb, type_hint=type_hint)
        tb.seek(0)
        return tb.read()


def make_read_object(
    type_hint: str, wrapper: ty.Callable[[ty.Any], T] = lambda o: o
) -> ty.Callable[[str], T]:
    def read_object(uri: str) -> T:
        return wrapper(
            CallableUnpickler(
                io.BytesIO(_get_bytes(lookup_blob_store(uri), uri, type_hint=type_hint))
            ).load()
        )

    return read_object


class _UnpickleFromUri:
    def __init__(self, uri: str):
        self.uri = uri  # serializable as a pure string for simplicity
        self._cached = None

    def __call__(self) -> object:
        # i don't believe there's any need for thread safety here, since pickle won't use threads.
        if self._cached is None:
            self._cached = pickle.load(
                io.BytesIO(
                    _get_bytes(
                        lookup_blob_store(self.uri),
                        self.uri,
                        type_hint="content-hash-addressed-pickle",
                    )
                )
            )
        return self._cached


def _upload_pickle_to_content_addressed_path(
    storage_root: str, obj, debug_name: str = ""
) -> _UnpickleFromUri:
    with io.BytesIO() as bio:
        pickle.dump(obj, bio)
        bio.seek(0)
        fs = lookup_blob_store(storage_root)
        base_uri = fs.join(
            storage_root,
            _CONTENT_HASH_ADDRESSED,
            "blob",
            nest(hash_using(bio, hashlib.sha256()).hexdigest()),
        )
        bytes_uri = fs.join(base_uri, "_bytes")
        fs.put(bytes_uri, bio)
        if debug_name:
            # this name is purely for debugging and affects no part of the runtime.
            fs.put(fs.join(base_uri, f"debugname_{debug_name}"), "goodbeef".encode())

    return _UnpickleFromUri(bytes_uri)


class SharedPickler:
    """A type of CallbackPickler.

    Name exists solely for debugging purposes.
    """

    def __init__(self, name: str = ""):
        self.name = name

    def __call__(self, obj: ty.Any) -> _UnpickleFromUri:
        # _get_container is lazy because we may want to register the pickler somewhere
        # before settling on the final destination of objects pickled.
        return _upload_pickle_to_content_addressed_path(_get_storage_root(), obj, self.name)
