"""Tools for using and creating locally-cached resources."""

import typing as ty
from pathlib import Path

from azure.core.exceptions import HttpResponseError, ResourceModifiedError
from azure.storage.blob import ContentSettings

from thds.core import files, fretry, hashing, link, log, scope, tmp

from .._progress import report_upload_progress
from .._upload import metadata_for_upload, upload_decision_and_settings
from ..conf import UPLOAD_FILE_MAX_CONCURRENCY
from ..download import download_or_use_verified
from ..errors import BlobNotFoundError
from ..fqn import AdlsFqn
from ..global_client import get_global_blob_container_client, get_global_fs_client
from ..ro_cache import Cache, global_cache
from .file_pointers import AdlsHashedResource, resource_from_path, resource_to_path

logger = log.getLogger(__name__)
_SLOW_CONNECTION_WORKAROUND = 14400  # seconds


# DOWNLOAD
def get_read_only(
    resource: AdlsHashedResource,
    local_path: ty.Optional[Path] = None,
    cache: Cache = global_cache(),
) -> Path:
    """Downloads a read-only resource if it is not already present in
    the cache or at the local_path.

    Because the resource includes a hash, we can save a lot of
    bandwidth if we can detect that it already is present locally.

    By default, downloads through the machine-global cache. Caching
    cannot be disabled, but the location of the cache can be changed.
    """
    cache = cache or global_cache()
    local_path = local_path or cache.path(resource.fqn)
    download_or_use_verified(
        get_global_fs_client(resource.fqn.sa, resource.fqn.container),
        resource.fqn.path,
        local_path,
        md5b64=resource.md5b64,
        cache=cache,
    )
    return local_path


# UPLOAD
UploadSrc = ty.Union[Path, bytes, ty.IO[ty.AnyStr], ty.Iterable[bytes]]


def _write_through_local_cache(local_cache_path: Path, data: UploadSrc) -> ty.Optional[Path]:
    @scope.bound
    def _try_write_through() -> bool:
        if isinstance(data, Path) and data.exists():
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
) -> ty.Optional[AdlsHashedResource]:
    """Uploads only if the remote does not exist or does not match
    md5.

    Always embeds md5 in upload if at all possible. In very rare cases
    it may not be possible for us to calculate one. Will always
    be possible if the passed data was a Path. If one can be
    calculated, an AdlsHashedResource is returned.

    Can write through a local cache, which may save you a download later.

    content_type and all upload_data_kwargs will be ignored if the file
    has already been uploaded and the md5 matches.
    """
    dest_ = AdlsFqn.parse(dest) if isinstance(dest, str) else dest
    if write_through_cache:
        _write_through_local_cache(write_through_cache.path(dest_), src)
        # we always use the original source file to upload, not the cached path,
        # because uploading from a shared location risks race conditions.

    blob_container_client = get_global_blob_container_client(dest_.sa, dest_.container)
    blob_client = blob_container_client.get_blob_client(dest_.path)
    decision = upload_decision_and_settings(blob_client.get_blob_properties, src)  # type: ignore [arg-type]
    if decision.upload_required:
        # set up some bookkeeping
        n_bytes = None  # if we pass 0 to upload_blob, it truncates the write now
        if isinstance(src, Path):
            n_bytes = src.stat().st_size
            src = scope.enter(open(src, "rb"))
        elif isinstance(src, bytes):
            n_bytes = len(src)

        adls_meta = metadata_for_upload()
        if "metadata" in upload_data_kwargs:
            adls_meta.update(upload_data_kwargs.pop("metadata"))

        upload_content_settings = decision.content_settings or ContentSettings()
        if content_type:
            upload_content_settings.content_type = content_type

        # we are now using blob_client instead of file system client
        # because blob client (as of 2024-06-24) does actually do
        # some one-step, atomic uploads, wherein there is not a separate
        # create/truncate action associated with an overwrite.
        # This is both faster, as well as simpler to reason about, and
        # in fact was the behavior I had been assuming all along...
        blob_client.upload_blob(
            report_upload_progress(ty.cast(ty.IO, src), str(dest_), n_bytes or 0),
            overwrite=True,
            length=n_bytes,
            content_settings=upload_content_settings,
            connection_timeout=_SLOW_CONNECTION_WORKAROUND,
            max_concurrency=UPLOAD_FILE_MAX_CONCURRENCY(),
            metadata=adls_meta,
            **upload_data_kwargs,
        )

    # if at all possible (if the md5 is known), return a resource containing it.
    if decision.content_settings and decision.content_settings.content_md5:
        return AdlsHashedResource.of(dest_, hashing.b64(decision.content_settings.content_md5))
    return None


def verify_remote_md5(resource: AdlsHashedResource) -> bool:
    try:
        props = (
            get_global_fs_client(resource.fqn.sa, resource.fqn.container)
            .get_file_client(resource.fqn.path)
            .get_file_properties()
        )
        if props.content_settings.content_md5:
            return hashing.b64(props.content_settings.content_md5) == resource.md5b64
    except HttpResponseError:
        return False
    except Exception:
        logger.exception("Unable to verify remote md5")
    return False


# DOWNLOAD if exists, else CREATE and UPLOAD


def verify_or_create_resource(
    resource_json_path: Path,
    get_adls_fqn: ty.Callable[[], AdlsFqn],
    creator: ty.Callable[[], Path],
    cache: ty.Optional[Cache] = global_cache(),
) -> AdlsHashedResource:
    """Return an MD5-verified resource if it already exists and
    matches the FQN _and_ the MD5 embedded in the resource JSON file.

    Does not download the actual resource if it can be verified to
    exist and match what is expected.

    If if does not exist or does not match, creates the resource and
    uploads it to the requested path as well.

    Basically an idempotent get-or-create pattern, applied to things
    that are more expensive to build than to upload, and that result
    in a single file.

    For this to work correctly, you will need the FQN itself to change
    based on some kind of key that can be considered unique to a
    particular version of the resource. Think of it like a cache
    key. For instance, if you have a resource that gets changed every
    time your library gets built, then your library version could be
    part of the Adls FQN.

    If running in CI, will create and upload your resource from CI,
    but will then raise an exception, since you need to commit the
    serialized resource to the resource_json_path.
    """
    remote_fqn = get_adls_fqn()  # this is lazy to allow partial application at a module level.
    if resource_json_path.exists():
        try:
            resource = resource_from_path(resource_json_path)
            try:
                if resource.fqn == remote_fqn:
                    if verify_remote_md5(resource):
                        return resource
                    else:
                        logger.info("Resource MD5 does not match; must recreate.")
                logger.info("Resource FQN does not match - it needs to be recreated.")
            except BlobNotFoundError:
                logger.info(f"Resource does not exist at {resource.fqn}; will create.")
            except Exception:
                logger.exception(f"Failed to get resource from {resource.fqn}, will recreate.")
        except Exception:
            logger.exception(f"Unable to parse a resource from {resource_json_path}; must recreate.")

    logger.info(f"Creating resource for {remote_fqn}...")
    created_path = creator()
    sz_mb = created_path.stat().st_size / 2**20  # 1 MB
    logger.info(
        f"Uploading created resource of size {sz_mb:.1f} MB to {remote_fqn} from {created_path} ..."
    )
    uploaded_resource = upload(remote_fqn, created_path, write_through_cache=cache)
    assert (
        uploaded_resource
    ), "Cannot create a shared resource without being able to calculate MD5 prior to upload."
    resource_to_path(resource_json_path, uploaded_resource, check_ci=True)
    return uploaded_resource
