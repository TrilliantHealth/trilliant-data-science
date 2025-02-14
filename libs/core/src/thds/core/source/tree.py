import concurrent.futures
import os
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from .. import logical_root, parallel
from .src import Source

_MAX_DOWNLOAD_PARALLELISM = 90


@dataclass
class SourceTree(os.PathLike):
    """Represent a fixed set of sources (with hashes where available) as a list of
    sources, plus the (optional) logical root of the tree, so that they can be 'unwrapped'
    as a local directory structure.
    """

    sources: ty.List[Source]
    higher_logical_root: str = ""
    # there may be cases where, rather than identifying the 'lowest common prefix'
    # of a set of sources/URIs, we may wish to represent a 'higher' root for the sake of some consuming system.
    # in those cases, this can be specified and we'll find the lowest common prefix _above_ that.

    def path(self) -> Path:
        """Return a local path to a directory that corresponds to the logical root.

        This incurs a download of _all_ sources explicitly represented by the list.
        """
        return Path(
            logical_root.find(
                (
                    str(p)
                    for _, p in parallel.failfast(
                        parallel.yield_all(
                            ((src, src.path) for src in self.sources),
                            executor_cm=concurrent.futures.ThreadPoolExecutor(
                                max_workers=_MAX_DOWNLOAD_PARALLELISM
                            ),
                        )
                    )
                ),
                self.higher_logical_root,
            )
        )

    def __fspath__(self) -> str:  # implement the os.PathLike protocol
        return str(self.path())
