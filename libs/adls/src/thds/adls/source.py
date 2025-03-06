import base64
import typing as ty
from functools import partial
from pathlib import Path

from thds.core import source
from thds.core.hashing import Hash, b64

from .cached_up_down import download_to_cache
from .fqn import AdlsFqn
from .resource import AdlsHashedResource
from .uri import resolve_any, resolve_uri


def _adls_uri_source_download_handler(uri: str) -> ty.Optional[source.Downloader]:
    fqn = resolve_uri(uri)
    if not fqn:
        return None

    def download(hash: ty.Optional[Hash]) -> Path:
        assert fqn
        if hash and hash.algo == "md5":
            # this 'extra' check just allows us to short-circuit a download
            # where the hash at this URI is known not to match what we expect.
            # It's no safer than the non-md5 hash check that Source performs after download.
            return download_to_cache(fqn, b64(hash.bytes))

        # we don't validate this hash, because we already have md5 validation
        # happening inside the download_to_cache function. the Source hash
        # is actually mostly for use by systems that want to do content addressing,
        # and not necessarily intended to be a runtime check in all scenarios.
        return download_to_cache(fqn)

    return download


source.register_download_handler("thds.adls", _adls_uri_source_download_handler)


def from_adls(
    uri_or_fqn_or_ahr: ty.Union[str, AdlsFqn, AdlsHashedResource], hash: ty.Optional[Hash] = None
) -> source.Source:
    """Flexible, public interface to creating Sources from any ADLS-like reference.

    Does NOT automatically fetch an MD5 hash from the ADLS URI if it's not provided. If
    you know you want to include that, combine this with `resource.get`:
    `source.from_adls(resource.get(uri))`
    """
    if isinstance(uri_or_fqn_or_ahr, AdlsHashedResource):
        fqn = uri_or_fqn_or_ahr.fqn
        res_hash = Hash("md5", base64.b64decode(uri_or_fqn_or_ahr.md5b64))
        if hash and hash != res_hash:
            raise ValueError(f"Resource Hash mismatch for {fqn}: {hash} != {res_hash}")
        hash = res_hash
    else:
        r_fqn = resolve_any(uri_or_fqn_or_ahr)
        if not r_fqn:
            raise ValueError(f"Could not resolve {uri_or_fqn_or_ahr} to an ADLS FQN")
        fqn = r_fqn

    return source.Source(str(fqn), hash)


source.register_from_uri_handler(
    "thds.adls", lambda uri: partial(from_adls, uri) if resolve_uri(uri) else None
)
