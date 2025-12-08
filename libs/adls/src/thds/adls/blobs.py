"""TODO: better organize the blobs-related modules in thds.adls"""

import heapq

from thds.core import log, source

from .fqn import AdlsFqn
from .impl import ADLSFileSystem
from .source import get_with_hash

_logger = log.getLogger(__name__)


def most_recent_blobs(blobs_fqn: AdlsFqn, top_n: int = 1) -> list[source.Source]:
    """Gets top n most recently-created blob in the directory at `blobs_fqn`."""
    _logger.info(f"Enumerating the most recent blobs in {blobs_fqn}")
    fs = ADLSFileSystem(blobs_fqn.sa, blobs_fqn.container)
    snapshots = fs.get_directory_info(blobs_fqn.path, recursive=False)
    if not snapshots:
        raise ValueError(f"No blobs found in {blobs_fqn}")
    top_blobs = heapq.nlargest(top_n, snapshots, key=lambda x: x.creation_time or -1)

    return [get_with_hash(blobs_fqn.root() / item.name) for item in top_blobs if item.name]
