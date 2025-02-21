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

from thds.core import hashing, log

from ..core.content_addressed import storage_content_addressed, wordybin_content_addressed
from ..core.serialize_paths import Downloader
from ..core.uris import active_storage_root, lookup_blob_store
from .pickles import UnpicklePathFromUri, UnpickleSimplePickleFromUri

logger = log.getLogger(__name__)
T = ty.TypeVar("T")


class Sha256B64PathStream:
    def local_to_remote(self, path: Path, sha256: str) -> None:
        """Return fully qualified remote information after put."""
        # lazily fetches the active storage root.
        full_remote_sha256 = storage_content_addressed(sha256, "sha256")
        lookup_blob_store(full_remote_sha256).putfile(path, full_remote_sha256)

    def get_downloader(self, remote_sha256: str) -> Downloader:
        return UnpicklePathFromUri(storage_content_addressed(remote_sha256, "sha256"))  # type: ignore # NamedTuple silliness


def _pickle_obj_and_upload_to_content_addressed_path(
    obj: object, debug_name: str = ""
) -> UnpickleSimplePickleFromUri:
    # active_storage_root is lazily fetched because we may want to register the pickler
    # somewhere before settling on the final destination of objects pickled.
    storage_root = active_storage_root()
    with io.BytesIO() as bio:
        pickle.dump(obj, bio)
        bio.seek(0)
        fs = lookup_blob_store(storage_root)
        bytes_uri, debug_uri = wordybin_content_addressed(
            hashing.Hash("sha256", hashing.hash_using(bio, hashlib.sha256()).digest()),
            storage_root,
            debug_name=f"objname_{debug_name}" if debug_name else "",
        )
        fs.putbytes(bytes_uri, bio, type_hint="application/octet-stream")
        if debug_uri:
            # this name is purely for debugging and affects no part of the runtime.
            fs.putbytes(debug_uri, "goodbeef".encode(), type_hint="text/plain")

    return UnpickleSimplePickleFromUri(bytes_uri)


class Sha256B64Pickler:
    """A type of CallbackPickler, intended for picklable objects that should be serialized
    as pure bytes and stored at a content-addressed URI. Only used (currently) by the
    ById/shared object serializer, most likely for something like a large dataframe.

    Name exists solely for debugging purposes.
    """

    def __init__(self, name: str = ""):
        self.name = name

    def __call__(self, obj: ty.Any) -> UnpickleSimplePickleFromUri:
        return _pickle_obj_and_upload_to_content_addressed_path(obj, self.name)
