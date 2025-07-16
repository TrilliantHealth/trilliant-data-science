import typing as ty
from functools import partial
from pathlib import Path

from thds.core import source
from thds.core.hashing import Hash

from . import cached, hashes, md5
from .errors import blob_not_found_translation
from .file_properties import get_file_properties
from .fqn import AdlsFqn
from .uri import resolve_any, resolve_uri


def _adls_uri_source_download_handler(uri: str) -> ty.Optional[source.Downloader]:
    fqn = resolve_uri(uri)
    if not fqn:
        return None

    def download(hash: ty.Optional[Hash]) -> Path:
        assert fqn
        # this 'extra' check just allows us to short-circuit a download
        # where the hash at this URI is known not to match what we expect.
        # It's no safer than the non-md5 hash check that Source performs after download.
        return cached.download_to_cache(fqn, expected_hash=hash)

    return download


source.register_download_handler("thds.adls", _adls_uri_source_download_handler)


def from_adls(uri_or_fqn: ty.Union[str, AdlsFqn], hash: ty.Optional[Hash] = None) -> source.Source:
    """Flexible, public interface to creating Sources from any ADLS-like reference.

    Does NOT automatically fetch a checksumming hash from the ADLS URI if it's not
    provided. If you know you want to include that, instead call:
    `source.get_with_hash(uri_or_fqn)`.
    """
    r_fqn = resolve_any(uri_or_fqn)
    if not r_fqn:
        raise ValueError(f"Could not resolve {uri_or_fqn} to an ADLS FQN")
    return source.Source(str(r_fqn), hash)


source.register_from_uri_handler(
    "thds.adls", lambda uri: partial(from_adls, uri) if resolve_uri(uri) else None
)


def get_with_hash(fqn_or_uri: ty.Union[AdlsFqn, str]) -> source.Source:
    """Creates a Source from a remote-only file, with MD5 or other hash.

    The file _must_ have a pre-existing hash!
    """
    fqn = AdlsFqn.parse(fqn_or_uri) if isinstance(fqn_or_uri, str) else fqn_or_uri
    with blob_not_found_translation(fqn):
        uri_hashes = hashes.extract_hashes_from_props(get_file_properties(fqn))
        if not uri_hashes:
            raise ValueError(
                f"ADLS file {fqn} must have a hash to use this function. "
                "If you know the hash, use `from_adls` with the hash parameter."
            )
        return from_adls(fqn, next(iter(uri_hashes.values())))


def with_md5b64(uri_or_fqn: ty.Union[str, AdlsFqn], *, md5b64: str = "") -> source.Source:
    """Meant for older use cases where we had an MD5"""
    return from_adls(uri_or_fqn, md5.to_hash(md5b64) if md5b64 else None)
