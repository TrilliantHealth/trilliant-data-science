"""This is an experimental, 'v2', API for mops that I feel like I
might like better than what we have.

However, while the serialized remote pointers can be read by the v1
code, newly-created Src or DestFiles using code imported from that API
will be serialized as v1 types, so you should attempt to use only the
new, v2 API if you want your local pointers to be serialized with the
new style.
"""
import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn, AdlsHashedResource, AdlsRoot
from thds.adls.md5 import md5_readable
from thds.core.hashing import b64

from ._remote_file_updn import URI_UPLOADERS
from ._uris import lookup_blob_store
from .adls_remote_files import _fqn, _sd_from_serialized
from .remote_file import DestFile, Serialized, SrcFile, StrOrPath

DestFileContext = ty.Callable[[str], DestFile]


def resource_from_srcdest(srcdest: ty.Union[SrcFile, DestFile]) -> AdlsHashedResource:
    """Will force upload if not already uploaded."""
    if isinstance(srcdest, SrcFile):
        srcdest._upload_if_not_already_remote()
    else:
        srcdest._force_serialization()
    sd = _sd_from_serialized(srcdest._serialized_remote_pointer)
    return AdlsHashedResource.of(_fqn(sd), sd["md5b64"])


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
        return AdlsHashedResource(fqn, md5b64=b64(md5_readable(file))).serialized  # type: ignore


_ADLS_URI_UPLOAD_V1 = "adls_uri_v1"  # do not ever change this key. it
# is serialized and doing so will invalidate all memoized DestFiles
# using it.
URI_UPLOADERS[_ADLS_URI_UPLOAD_V1] = _upload_and_represent_v2


def direct_dest(fqn: AdlsFqn, local_file: StrOrPath) -> DestFile:
    return DestFile((_ADLS_URI_UPLOAD_V1, str(fqn)), local_file)


def destfile_context(
    storage_root: ty.Union[AdlsFqn, AdlsRoot],
    local_root: Path,
) -> DestFileContext:
    local_root.mkdir(exist_ok=True, parents=True)
    return lambda rel: direct_dest(storage_root.join(rel), local_root / rel)
