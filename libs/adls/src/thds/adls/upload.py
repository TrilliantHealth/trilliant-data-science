"""API for uploading files to Azure Data Lake Storage (ADLS) Gen2.

We hash anything that we possibly can, since it's a fast verification step that we
can do later during downloads.
"""

import subprocess
import typing as ty
from pathlib import Path

from azure.core.exceptions import ResourceModifiedError
from azure.storage.blob import ContentSettings

from thds.core import files, fretry, link, log, scope, source, tmp

from . import azcopy, hashes
from ._progress import report_upload_progress
from ._upload import upload_decision_and_metadata
from .conf import UPLOAD_FILE_MAX_CONCURRENCY
from .fqn import AdlsFqn
from .global_client import get_global_blob_container_client
from .ro_cache import Cache

logger = log.getLogger(__name__)
_SLOW_CONNECTION_WORKAROUND = 14400  # seconds


UploadSrc = ty.Union[Path, bytes, ty.IO[ty.AnyStr], ty.Iterable[bytes]]


def _write_through_local_cache(local_cache_path: Path, data: UploadSrc) -> ty.Optional[Path]:
    @scope.bound
    def _try_write_through() -> bool:
        if isinstance(data, Path) and data.exists():
            # we don't do hard or soft links because they share file permissions,
            # and it's not up to us to change permissions on the src file.
            link.link_or_copy(data, local_cache_path, "ref")
            return True

        out = scope.enter(tmp.temppath_same_fs(local_cache_path))
        if hasattr(data, "read") and hasattr(data, "seek"):
            with open(out, "wb") as f:
                f.write(data.read())  # type: ignore
            data.seek(0)  # type: ignore
            link.link_or_copy(out, local_cache_path)
            return True

        if isinstance(data, bytes):
            with open(out, "wb") as f:
                f.write(data)
            link.link_or_copy(out, local_cache_path)
            return True

        return False

    if _try_write_through():
        try:
            # it's a reflink or a copy, so the cache now owns its copy
            # and we don't want to allow anyone to write to its copy.
            files.set_read_only(local_cache_path)
            return local_cache_path

        except FileNotFoundError:
            # may have hit a race condition.
            # don't fail upload just because we couldn't write through the cache.
            pass
    return None


@scope.bound
@fretry.retry_sleep(
    # ADLS lib has a bug where parallel uploads of the same thing will
    # hit a race condition and error.  this will detect that scenario
    # and avoid re-uploading as well.
    fretry.is_exc(ResourceModifiedError),
    fretry.expo(retries=5),
)
def upload(
    dest: ty.Union[AdlsFqn, str],
    src: UploadSrc,
    write_through_cache: ty.Optional[Cache] = None,
    *,
    content_type: str = "",
    **upload_data_kwargs: ty.Any,
) -> source.Source:
    """Uploads only if the remote does not exist or does not match
    xxhash.

    Always embeds xxhash in the blob metadata if at all possible. In very rare cases
    it may not be possible for us to calculate one. Will always be possible if the passed
    data was a Path. If one can be calculated, it will be returned in the Source.

    Can write through a local cache, which may save you a download later.

    content_type and all upload_data_kwargs will be ignored if the file
    has already been uploaded and the hash matches.
    """
    dest_ = AdlsFqn.parse(dest) if isinstance(dest, str) else dest
    if write_through_cache:
        _write_through_local_cache(write_through_cache.path(dest_), src)
        # we always use the original source file to upload, not the cached path,
        # because uploading from a shared location risks race conditions.

    blob_container_client = get_global_blob_container_client(dest_.sa, dest_.container)
    blob_client = blob_container_client.get_blob_client(dest_.path)
    decision = upload_decision_and_metadata(blob_client.get_blob_properties, src)  # type: ignore [arg-type]

    def source_from_meta() -> source.Source:
        best_hash = next(iter(hashes.extract_hashes_from_metadata(decision.metadata)), None)
        if isinstance(src, Path):
            assert best_hash, "A hash should always be calculable for a local path."
            return source.from_file(src, hash=best_hash, uri=str(dest_))

        return source.from_uri(str(dest_), hash=best_hash)

    if decision.upload_required:
        # set up some bookkeeping
        n_bytes = None  # if we pass 0 to upload_blob, it truncates the write now
        bytes_src: ty.Union[bytes, ty.IO, ty.Iterable[bytes]]
        if isinstance(src, Path):
            n_bytes = src.stat().st_size
            bytes_src = scope.enter(open(src, "rb"))
        elif isinstance(src, bytes):
            n_bytes = len(src)
            bytes_src = src
        else:
            bytes_src = src

        if "metadata" in upload_data_kwargs:
            decision.metadata.update(upload_data_kwargs.pop("metadata"))

        if azcopy.upload.should_use_azcopy(n_bytes or 0) and isinstance(src, Path):
            logger.info("Using azcopy to upload %s to %s", src, dest_)
            try:
                azcopy.upload.run(
                    azcopy.upload.build_azcopy_upload_command(
                        src, dest_, content_type=content_type, metadata=decision.metadata, overwrite=True
                    ),
                    dest_,
                    n_bytes or 0,
                )
                return source_from_meta()

            except subprocess.SubprocessError:
                logger.warning("Azcopy upload failed, falling back to SDK upload")

        upload_content_settings = ContentSettings()
        if content_type:
            upload_content_settings.content_type = content_type

        # we are now using blob_client instead of file system client
        # because blob client (as of 2024-06-24) does actually do
        # some one-step, atomic uploads, wherein there is not a separate
        # create/truncate action associated with an overwrite.
        # This is both faster, as well as simpler to reason about, and
        # in fact was the behavior I had been assuming all along...
        blob_client.upload_blob(
            report_upload_progress(ty.cast(ty.IO, bytes_src), str(dest_), n_bytes or 0),
            overwrite=True,
            length=n_bytes,
            content_settings=upload_content_settings,
            connection_timeout=_SLOW_CONNECTION_WORKAROUND,
            max_concurrency=UPLOAD_FILE_MAX_CONCURRENCY(),
            metadata=decision.metadata,
            **upload_data_kwargs,
        )

    return source_from_meta()
