import typing as ty
from pathlib import Path

from thds.adls import source
from thds.core import parallel
from thds.core.source.tree import SourceTree
from thds.core.thunks import thunking

from .download import download_or_use_verified
from .fqn import AdlsFqn
from .global_client import get_global_fs_client
from .impl import ADLSFileSystem
from .resource.up_down import AdlsHashedResource, upload
from .ro_cache import global_cache
from .uri import UriIsh, parse_any


def download_to_cache(fqn_or_uri: UriIsh, md5b64: str = "") -> Path:
    """Downloads directly to the cache and returns a Path to the read-only file.

    This will allow you to download a file 'into' the cache even if
    you provide no MD5 and the remote file properties does not have
    one. However, future attempts to reuse the cache will force a
    re-download if no MD5 is available at that time.
    """
    fqn = parse_any(fqn_or_uri)
    cache_path = global_cache().path(fqn)
    download_or_use_verified(
        get_global_fs_client(fqn.sa, fqn.container), fqn.path, cache_path, md5b64, cache=global_cache()
    )
    return cache_path


def upload_through_cache(dest: UriIsh, src_path: Path) -> AdlsHashedResource:
    """Return an AdlsHashedResource, since by definition an upload through the cache must have a known checksum.

    Uses global client, which is pretty much always what you want.
    """
    assert src_path.is_file(), "src_path must be a file."
    resource = upload(dest, src_path, write_through_cache=global_cache())
    assert resource, "MD5 should always be calculable for a local path."
    return resource


def download_directory(fqn: AdlsFqn) -> Path:
    """Download a directory from an AdlsFqn.

    If you know you only need to download a single file, use download_to_cache.
    """
    fs = ADLSFileSystem(fqn.sa, fqn.container)
    cached_dir_root = global_cache().path(fqn)
    fs.fetch_directory(fqn.path, cached_dir_root)
    assert cached_dir_root.is_dir(), "Directory should have been downloaded to the cache."
    return cached_dir_root


def _yield_all_file_paths(dir_path: Path) -> ty.Iterator[Path]:
    for item in dir_path.iterdir():
        if item.is_dir():  # recur
            yield from _yield_all_file_paths(item)
        elif item.is_file():  # yield
            yield item


def upload_directory_through_cache(dest: UriIsh, src_path: Path) -> SourceTree:
    if not src_path.is_dir():
        raise ValueError(f"If you want to upload a file, use {upload_through_cache.__name__} instead")

    dest = parse_any(dest)

    upload_thunks = [
        thunking(upload_through_cache)(dest / str(file_path.relative_to(src_path)), file_path)
        for file_path in _yield_all_file_paths(src_path)
    ]

    return SourceTree(
        sources=list(
            map(
                source.from_adls,
                parallel.yield_results(upload_thunks, named="upload_directory_through_cache"),
            )
        )
    )
