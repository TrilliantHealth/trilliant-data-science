"""v2 ADLS shims for the DestFile/SrcFile abstractions."""
import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn, AdlsRoot, errors, resource
from thds.adls.global_client import get_global_client
from thds.core.hashing import b64
from thds.core.types import StrOrPath

from ....srcdest.remote_file import DestFile, Serialized, SrcFile
from .destf import _upload_and_represent_v2
from .download import ADLS_DOWNLOAD_V1
from .parse_serialized import read_possible_serialized, resource_from_serialized


def _validate_remote_srcfile(fqn: AdlsFqn, md5b64: ty.Optional[str] = "", **kwargs) -> resource.AHR:
    with errors.blob_not_found_translation(fqn):
        adls_md5 = (
            get_global_client(fqn.sa, fqn.container)
            .get_file_client(fqn.path)
            .get_file_properties()
            .content_settings.content_md5
        )
        if adls_md5:
            adls_md5 = b64(adls_md5)  # don't b64 an empty string - you'll get something weird
        if md5b64 and md5b64 != adls_md5:
            # don't tolerate a mismatch between someone's explicit
            # expectation and what ADLS says.  in some cases,
            # people won't know the md5 and they'll want to just
            # trust what's on ADLS, but in the ideal caseswe'll
            # have recorded what we expect and this will help us
            # identify mutated blobs.
            raise ValueError(f"Mismatched MD5 for ADLS blob - expected {md5b64} but ADLS has {adls_md5}")
        if adls_md5:
            # finally - make sure we embed this in the serialization
            # if we can get it - this will prevent errors stemming
            # from incorrectly mutated data.
            return resource.AHR(fqn, adls_md5)
    return resource.AHR(fqn, "")


def _srcfile_from_serialized(serialized: Serialized) -> SrcFile:
    """Make a SrcFile directly from a known Serialized remote pointer.

    Because this is statically type-checked, a TypeError will be
    raised if the Serialized object cannot be parsed.
    """
    ahr = resource_from_serialized(serialized)
    return SrcFile(
        ADLS_DOWNLOAD_V1, Serialized(_validate_remote_srcfile(ahr.fqn, ahr.md5b64).serialized)
    )


def src_from_dest(destfile: DestFile) -> SrcFile:
    """Directly translate a DestFile into a SrcFile.

    If the DestFile has not been uploaded, we will force its upload.

    Skips writing a serialized pointer locally.
    """
    destfile._force_serialization()
    return _srcfile_from_serialized(destfile._serialized_remote_pointer)


def src(fqn: AdlsFqn, md5b64: str = "") -> SrcFile:
    """A v2 API that serializes a little bit more carefully for better forward-compatibility.

    Ensures that the file actually exists on ADLS, and verifies its
    md5b64 if the file was uploaded with one, to maximize our chances
    of deterministic computing.
    """
    ahr = _validate_remote_srcfile(fqn, md5b64)
    return SrcFile(ADLS_DOWNLOAD_V1, Serialized(ahr.serialized))


def local_src(fqn: AdlsFqn, local_path: StrOrPath) -> SrcFile:
    return SrcFile(
        ADLS_DOWNLOAD_V1,
        local_path=local_path,
        uploader=lambda lp: _upload_and_represent_v2(str(fqn), lp),
    )


class SrcFileContext:
    def __init__(self, storage_root: ty.Union[AdlsFqn, AdlsRoot], local_root: StrOrPath = "."):
        self.storage_root = storage_root
        self.local_root = Path(local_root)

    def __call__(self, relpath: str, local_path: StrOrPath = "", md5b64: str = "") -> SrcFile:
        if local_path:
            if not Path(local_path).exists():
                local_path = self.local_root / local_path
            if not Path(local_path).exists():
                raise FileNotFoundError(f"Could not find local file {local_path}")
            return local_src(self.storage_root / relpath, local_path)
        return src(self.storage_root / relpath, md5b64)


def load_srcfile(path: StrOrPath) -> ty.Optional[SrcFile]:
    ahr = read_possible_serialized(path)
    if not ahr:
        return None
    return src(ahr.fqn, ahr.md5b64)


def fqn_relative_to_src(
    srcfile: SrcFile,
    *path_parts: str,
    up: int = 0,
) -> AdlsFqn:
    assert path_parts, "Must specify a relative path"
    fqn = resource_from_serialized(srcfile._serialized_remote_pointer).fqn
    while up > 0:
        fqn = fqn.parent
        up -= 1
    return fqn.join(*path_parts)
