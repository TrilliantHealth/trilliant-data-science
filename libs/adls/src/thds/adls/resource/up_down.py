"""Tools for using and creating locally-cached resources."""
import shutil
import typing as ty
from pathlib import Path

from thds.core import log, scope

from .._upload import upload_decision_and_settings
from ..download import BlobNotFoundError, download_or_use_verified
from ..fqn import AdlsFqn
from ..global_client import get_global_client
from ..link import link, set_read_only
from ..ro_cache import Cache, global_cache
from .file_pointers import AdlsHashedResource, resource_from_path, resource_to_path

logger = log.getLogger(__name__)


# DOWNLOAD
def get_read_only(
    resource: AdlsHashedResource,
    local_path: ty.Optional[Path] = None,
    cache: ty.Optional[Cache] = None,
) -> Path:
    """Downloads a read-only resource if it is not already present."""
    cache = cache or global_cache()
    local_path = local_path or cache.path(resource.fqn)
    download_or_use_verified(
        get_global_client(resource.fqn.sa, resource.fqn.container),
        resource.fqn.path,
        local_path,
        md5b64=resource.md5b64,
        cache=cache,
    )
    return local_path


# UPLOAD
UploadSrc = ty.Union[Path, bytes, ty.IO[ty.AnyStr], ty.Iterable[bytes]]


def _write_through_local_cache(local_cache_path: Path, data: UploadSrc) -> ty.Optional[Path]:
    if isinstance(data, Path) and data.exists():
        link_type = link(data, local_cache_path, "ref")
        assert link_type in {"ref", ""}, link_type
        if not link_type:
            shutil.copyfile(data, local_cache_path)
        assert local_cache_path.exists(), local_cache_path
        # it's a reflink or a copy, so the cache now owns its copy
        # and we don't want to allow anyone to write to its copy.
        set_read_only(local_cache_path)
        return local_cache_path
    if hasattr(data, "read") and hasattr(data, "seek"):
        if local_cache_path.exists():
            local_cache_path.unlink()
        with local_cache_path.open("wb") as f:
            f.write(data.read())  # type: ignore
            data.seek(0)  # type: ignore
        set_read_only(local_cache_path)
        return local_cache_path
    if isinstance(data, bytes):
        if local_cache_path.exists():
            local_cache_path.unlink()
        with local_cache_path.open("wb") as f:
            f.write(data)
        set_read_only(local_cache_path)
        return local_cache_path
    return None


@scope.bound
def upload(
    fqn: AdlsFqn, data: UploadSrc, write_through_cache: ty.Optional[Cache] = None
) -> ty.Optional[AdlsHashedResource]:
    """Uploads only if the remote does not exist or does not match
    md5.

    Always embeds md5 in upload if at all possible. In very rare cases
    it may not be possible for us to calculate one. If one was
    calculated, an AdlsHashedResource is returned.

    Can write through a local cache if provided.
    """
    if write_through_cache:
        data = _write_through_local_cache(write_through_cache.path(fqn), data) or data
        # we can now upload from the local cache path, which has the
        # advantage (over some kinds of readable byte iterables) of
        # guaranteeing that we can extract an md5.

    fs_client = get_global_client(fqn.sa, fqn.container).get_file_client(fqn.path)
    decision = upload_decision_and_settings(fs_client, data)
    if decision.upload_required:
        if isinstance(data, Path):
            data = scope.enter(open(data, "rb"))
        fs_client.upload_data(data, overwrite=True, content_settings=decision.content_settings)
    if decision.content_settings and decision.content_settings.content_md5:
        return AdlsHashedResource.of(fqn, decision.content_settings.content_md5)
    return None


# DOWNLOAD if exists, else CREATE and UPLOAD


def get_or_create_shared_resource(
    resource_json_path: Path,
    describe: ty.Callable[[], ty.Tuple[Path, AdlsFqn]],
    creator: ty.Callable[[], Path],
    cache: ty.Optional[Cache] = None,
) -> Path:
    local_path, remote_fqn = describe()
    if resource_json_path.exists():
        try:
            resource = resource_from_path(resource_json_path)
            try:
                if resource.fqn == remote_fqn:
                    return get_read_only(resource, local_path, cache=cache)
            except BlobNotFoundError:
                logger.info(f"Resource does not exist at {resource.fqn}; will create.")
            except Exception:
                logger.exception(f"Failed to get resource from {resource.fqn}, will recreate.")
        except Exception:
            logger.exception(f"Unable to parse a resource from {resource_json_path}; must recreate.")

    shutil.move(str(creator()), str(local_path))
    uploaded_resource = upload(resource.fqn, local_path, write_through_cache=cache)
    assert (
        uploaded_resource
    ), "Cannot create a shared resource without being able to calculate MD5 prior to upload."
    resource_to_path(resource_json_path, uploaded_resource)
    return local_path
