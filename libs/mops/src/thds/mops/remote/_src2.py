import json

from thds.adls import AdlsFqn, AdlsHashedResource

from ._remote_file_updn import DOWNLOADERS
from .adls_remote_files import _download_serialized, _fqn, _sd_from_serialized, _validate_remote_srcfile
from .remote_file import DestFile, Serialized, SrcFile

# never, ever change this key, since it is serialized along with the URI in SrcFiles
_ADLS_DOWNLOAD_V1 = "adls_serialized_v1"
DOWNLOADERS[_ADLS_DOWNLOAD_V1] = _download_serialized


def _srcfile_from_serialized(serialized: Serialized) -> SrcFile:
    """Make a SrcFile directly from a known Serialized remote pointer.

    Because this is statically type-checked, a TypeError will be
    raised if the Serialized object cannot be parsed.
    """
    _validate_remote_srcfile(**_sd_from_serialized(serialized))
    return SrcFile(_ADLS_DOWNLOAD_V1, serialized)


def src_from_dest(destfile: DestFile) -> SrcFile:
    """Directly translate a DestFile into a SrcFile.

    If the DestFile has not been uploaded, we will force its upload.

    Skips writing a serialized pointer locally.
    """
    destfile._force_serialization()
    return _srcfile_from_serialized(destfile._serialized_remote_pointer)


def remote_only(fqn: AdlsFqn, md5b64: str = "") -> SrcFile:
    """A v2 API that serializes a little bit more carefully for better forward-compatibility.

    Ensures that the file actually exists on ADLS, and verifies its
    md5b64 if the file was uploaded with one, to maximize our chances
    of deterministic computing.
    """
    d = _validate_remote_srcfile(fqn.sa, fqn.container, fqn.path, md5b64)
    md5b64 = md5b64 or str(d.get("md5b64") or "")
    return SrcFile(
        _ADLS_DOWNLOAD_V1,
        Serialized(
            AdlsHashedResource(fqn, md5b64).serialized if md5b64 else json.dumps(dict(uri=str(fqn)))
        ),
    )


def fqn_relative_to_src(
    srcfile: SrcFile,
    *path_parts: str,
    up: int = 0,
) -> AdlsFqn:
    assert path_parts, "Must specify a relative path"
    fqn = _fqn(_sd_from_serialized(srcfile._serialized_remote_pointer))
    while up > 0:
        fqn = fqn.parent
        up -= 1
    return fqn.join(*path_parts)
