"""Context-local, content-aware ser/de from/to a known URI prefix.

Basically, we take some generic pickle utilities and stitch them together into something
that efficiently serializes object graphs to a combination of locations at some URI prefix,
such that they are self-deserializing (via CallableUnpickler) on the other side.
"""
import hashlib
import io
import pickle
import typing as ty
from pathlib import Path

from thds.core.hashing import hash_using
from thds.core.log import getLogger
from thds.core.stack_context import StackContext

from ...._utils.human_b64 import encode
from ...core.serialize_paths import Downloader
from ...core.uris import lookup_blob_store
from ..pickles import UnpicklePathFromUri, UnpickleSimplePickleFromUri

logger = getLogger(__name__)
T = ty.TypeVar("T")


SHA256_B64_ADDRESSED = "sha256-b64-addressed"
# we can save on storage and simplify lots of internals if we just
# hash all blobs and upload them to a key that is their hash.

# DEFERRED_STORAGE_ROOT is meant as a global, non-semantic URI prefix.
# In other words, it should have nothing to do with your application
DEFERRED_STORAGE_ROOT: StackContext[str] = StackContext("STORAGE_ROOT", default="")
# We use StackContext in order to perform lazy dependency injection - it's possible
# that various configurations may arise requiring the root location to need to be changed
# for a specific function invocation, and this allows the configuration to take hold naturally
# without lots of additional bookkeeping.

# objects referencing this StackContext must be used in the same thread where they were created.


def _get_storage_root() -> str:
    storage_root = DEFERRED_STORAGE_ROOT()
    assert storage_root, "STORAGE_ROOT must be set before SharedPickler is used."
    return storage_root


def _sha256_addressed(sha256_of_some_kind: str) -> str:
    return f"{_get_storage_root()}/{SHA256_B64_ADDRESSED}/{sha256_of_some_kind}"


class Sha256B64PathStream:
    def local_to_remote(self, path: Path, key: str):
        """Return fully qualified remote information after put."""
        full_remote_key = _sha256_addressed(key)
        lookup_blob_store(full_remote_key).putfile(path, full_remote_key)

    def get_downloader(self, remote_key: str) -> Downloader:
        return UnpicklePathFromUri(_sha256_addressed(remote_key))  # type: ignore # NamedTuple silliness


def _pickle_and_upload_to_content_addressed_path(
    storage_root: str, obj, debug_name: str = ""
) -> UnpickleSimplePickleFromUri:
    with io.BytesIO() as bio:
        pickle.dump(obj, bio)
        bio.seek(0)
        fs = lookup_blob_store(storage_root)
        base_uri = fs.join(
            storage_root,
            SHA256_B64_ADDRESSED,
            encode(hash_using(bio, hashlib.sha256()).digest()),
        )
        bytes_uri = fs.join(base_uri, "_bytes")
        # corresponds with '_bytes' as used in `serialize_paths.py`
        fs.putbytes(bytes_uri, bio)
        if debug_name:
            # this name is purely for debugging and affects no part of the runtime.
            fs.putbytes(fs.join(base_uri, f"objname_{debug_name}"), "goodbeef".encode())

    return UnpickleSimplePickleFromUri(bytes_uri)


class Sha256B64Pickler:
    """A type of CallbackPickler.

    Name exists solely for debugging purposes.
    """

    def __init__(self, name: str = ""):
        self.name = name

    def __call__(self, obj: ty.Any) -> UnpickleSimplePickleFromUri:
        # _get_storage_root is lazy because we may want to register the pickler somewhere
        # before settling on the final destination of objects pickled.
        return _pickle_and_upload_to_content_addressed_path(_get_storage_root(), obj, self.name)
