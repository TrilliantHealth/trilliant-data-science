from datetime import datetime, timedelta, timezone
from pathlib import Path

from thds.core import config

CONTROL_CACHE_TTL_IN_SECONDS = config.item(
    "thds.mops.pure.control_cache_ttl_in_seconds",
    default=60 * 60 * 8,  # 8 hours
    parse=int,
)
# Set the above to 0 in order to specifically refresh read-path caching of
# mops-created files. Set it to a negative value to bypass caching completely
# (which also results in hashes not being checked). This can apply to a local
# (stack) context, or can apply globally to the process. The former may be used
# selectively within mops for issues of known correctness, e.g. locks, whereas
# the latter will be useful for debugging any cases where files have been
# remotely deleted.


def exists_with_expiry(cache_path: Path, cache_ttl_in_seconds: int) -> bool:
    try:
        stat = cache_path.stat()
    except FileNotFoundError:
        return False

    last_modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    if datetime.now(timezone.utc) - last_modified_at >= timedelta(seconds=cache_ttl_in_seconds):
        cache_path.unlink(missing_ok=True)
        return False

    return True
