from pathlib import Path

from .download import download_or_use_verified
from .fqn import AdlsFqn
from .global_client import get_global_client
from .resource.up_down import AdlsHashedResource, upload
from .ro_cache import global_cache


def download_to_cache(fqn: AdlsFqn, md5b64: str = "") -> Path:
    """Downloads directly to the cache and returns a Path to the read-only file.

    This will allow you to download a file 'into' the cache even if
    you provide no MD5 and the remote file properties does not have
    one. However, future attempts to reuse the cache will force a
    re-download if no MD5 is available at that time.
    """
    cache_path = global_cache().path(fqn)
    download_or_use_verified(
        get_global_client(fqn.sa, fqn.container), fqn.path, cache_path, md5b64, cache=global_cache()
    )
    return cache_path


def upload_through_cache(fqn: AdlsFqn, local_path: Path) -> AdlsHashedResource:
    """Return an AdlsHashedResource, since by definition an upload through the cache must have a known checksum.

    Uses global client, which is pretty much always what you want.
    """
    resource = upload(fqn, local_path, write_through_cache=global_cache())
    assert resource, "MD5 should always be calculable for a local path."
    return resource
