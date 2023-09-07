"""This is an experimental, 'v2', API for mops that I feel like I
might like better than what we have.
"""
import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn, AdlsRoot, resource
from thds.adls.md5 import md5_readable
from thds.core.hashing import b64

from ....srcdest.remote_file import DestFile, Serialized, SrcFile, StrOrPath
from ....srcdest.up_down import URI_UPLOADERS
from ...core.uris import lookup_blob_store
from .parse_serialized import resource_from_serialized


def to_resource(srcdest: ty.Union[SrcFile, DestFile]) -> resource.AHR:
    """Will force upload if not already uploaded."""
    if isinstance(srcdest, SrcFile):
        srcdest._upload_if_not_already_remote()
    else:
        srcdest._force_serialization()
    ahr = resource_from_serialized(srcdest._serialized_remote_pointer)
    assert ahr.md5b64, "This Src or DestFile has no hash and cannot be converted into a Resource."
    return ahr


def _upload_and_represent_v2(uri: str, local_src: StrOrPath) -> Serialized:
    fqn = AdlsFqn.parse(uri)
    with open(local_src, "rb") as file:
        uri = str(fqn)
        lookup_blob_store(uri).putbytes(uri, file)
        file.seek(0)
        # The primary reason for representing the md5 inside the
        # serialized file pointer is to add greater confidence in
        # memoization. This prevents memoizing results that are based
        # on a shared blob path but different blob contents.
        #
        # We use md5 base64 so this is easy to verify against ADLS
        # without downloading the file.  we do not currently make use
        # of this validation but we could in the future.
        return resource.of(fqn, md5b64=b64(md5_readable(file))).serialized  # type: ignore


_ADLS_URI_UPLOAD_V1 = "adls_uri_v1"  # do not ever change this key. it
# is serialized and doing so will invalidate all memoized DestFiles
# using it.
URI_UPLOADERS[_ADLS_URI_UPLOAD_V1] = _upload_and_represent_v2


def dest(fqn: AdlsFqn, local_file: StrOrPath = "") -> DestFile:
    return DestFile((_ADLS_URI_UPLOAD_V1, str(fqn)), local_file)


class DestFileContext:
    """Aligns an orchestrator-local directory structure with a remote directory structure.

    Do not use this for remote-only DestFiles, which have no local directory structure.
    """

    def __init__(self, storage_root: ty.Union[AdlsFqn, AdlsRoot], local_root: StrOrPath = "."):
        self.storage_root = storage_root
        self.local_root = Path(local_root)

    def __call__(self, rel: str) -> DestFile:
        self.local_root.mkdir(exist_ok=True, parents=True)
        return dest(self.storage_root / rel, self.local_root / rel)
