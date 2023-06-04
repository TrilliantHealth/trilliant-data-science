import typing as ty
from pathlib import Path

from thds.adls import AdlsFqn, download, ro_cache
from thds.adls.global_client import get_global_client
from thds.core import log

logger = log.getLogger(__name__)


def make_caching_installer(
    data_fqn: AdlsFqn,
    md5b64: str,
    cache: download.Cache = ro_cache.global_cache(),  # noqa: B008
) -> ty.Callable[[], Path]:
    """Creates a thunk suitable for embedding in a Lazy-loading
    context.

    Though the download itself will be cached, you probably still only
    want to call this once per application, because the necessary
    local and remote md5 checks take nonzero time.

    For many use cases, you may not need to manually specify the
    cache, since the default machine-global cache will be sufficient.
    """
    assert md5b64, "Must specify an MD5 for the data so that this cannot use the wrong data silently."

    def cached_install() -> Path:
        cache_path = cache.path(data_fqn)
        cached = download.download_or_use_verified(
            get_global_client(data_fqn.sa, data_fqn.container),
            data_fqn.path,
            cache_path,
            md5b64=md5b64,
            cache=cache,
        )
        if cached:
            logger.info(f"{data_fqn} already present on local machine.")
        return cache_path

    return cached_install
