"""Tools for using and creating locally-cached resources."""
import shutil
import typing as ty
from pathlib import Path

from thds.core import log
from thds.core.hashing import b64

from .._upload import upload
from ..download import BlobNotFoundError, download_or_use_verified
from ..fqn import AdlsFqn
from ..global_client import get_global_client
from ..md5 import md5_readable
from ..ro_cache import Cache, global_cache
from .file_pointers import AdlsHashedResource, resource_from_path, resource_to_path

logger = log.getLogger(__name__)


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
    with open(local_path, "rb") as f:
        md5b64 = b64(md5_readable(f))
    resource = AdlsHashedResource.of(remote_fqn, md5b64)
    upload(resource.fqn, local_path, write_through_cache=cache)
    resource_to_path(resource_json_path, resource)
    return local_path
