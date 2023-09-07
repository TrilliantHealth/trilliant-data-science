"""An experimental feature being used by NPPES.

This probably does not need to be ADLS-specific, if Src/DestFile natively supported transformation into URIs.
"""
import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn
from thds.core import log

from ....srcdest.remote_file import Serialized, SrcFile, StrOrPath, Uploader
from .destf import _upload_and_represent_v2
from .srcf import ADLS_DOWNLOAD_V1, load_srcfile

logger = log.getLogger(__name__)


def _mirrored_srcfile_uploader(uri: str, mirror_root: StrOrPath, mirror_ext: str) -> Uploader:
    def _upload_and_mirror(rel_path: StrOrPath) -> Serialized:
        serialized = _upload_and_represent_v2(uri, rel_path)
        mirror_path = Path(mirror_root) / (str(rel_path) + mirror_ext)
        mirror_path.parent.mkdir(exist_ok=True, parents=True)
        logger.info(f"Writing mirror srcfile to {mirror_path}")
        with open(mirror_path, "w") as mf:
            mf.write(serialized.rstrip("\n") + "\n")
        return serialized

    return _upload_and_mirror


def mirrored_srcfile_context(
    adls_root: ty.Union[AdlsFqn, str],
    src_mirror_root: StrOrPath,
    mirror_ext: str = ".adls",
) -> ty.Callable[[StrOrPath], SrcFile]:
    """A SrcFile creator that automatically creates a local '.adls'
    mirror for any file that you upload.

    Handy for DVC-like workflows where you want to commit a pointer to
    your repository after you perform the one-time upload of the
    original file.
    """
    _adls_root = AdlsFqn.parse(str(adls_root))
    local_src_root = Path(src_mirror_root)

    def make_mirrored_srcfile(local_path: StrOrPath) -> SrcFile:
        already_remote_srcfile = load_srcfile(str(local_path))
        if already_remote_srcfile:
            return already_remote_srcfile

        return SrcFile(
            ADLS_DOWNLOAD_V1,
            uploader=_mirrored_srcfile_uploader(
                # this can be an old-style function that uploads
                # because it will never get serialized inside a
                # SrcFile, since the upload always must happen before
                # serialization, and the uploader is then deleted.
                str(_adls_root.join(str(local_path))),
                local_src_root,
                mirror_ext,
            ),
            local_path=str(local_path),
        )

    return make_mirrored_srcfile
