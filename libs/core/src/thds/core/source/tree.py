import concurrent.futures
import os
import shutil
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from .. import cm, link, logical_root, parallel, thunks, types
from .src import Source

_MAX_PARALLELISM = 90


def _logical_tree_replication_operations(
    local_paths: ty.Iterable[Path], logical_local_root: Path, dest_dir: Path
) -> ty.Tuple[Path, ty.List[ty.Tuple[Path, Path]]]:
    """
    Pure function that determines required copy operations.
    Returns (logical_dest, list of (src, dest) pairs)
    """
    logical_dest = dest_dir / logical_local_root.name
    operations = [(src, logical_dest / src.relative_to(logical_local_root)) for src in local_paths]
    return logical_dest, operations


def replicate_logical_tree(
    local_paths: ty.Iterable[Path],
    logical_local_root: Path,
    dest_dir: Path,
    copy: ty.Callable[[Path, Path], ty.Any] = link.cheap_copy,
    executor_cm: ty.Optional[ty.ContextManager[concurrent.futures.Executor]] = None,
) -> Path:
    """
    Replicate only the specified files from logical_root into dest_dir.
    Returns the path to the logical root in the new location.
    """
    logical_dest, operations = _logical_tree_replication_operations(
        local_paths, logical_local_root, dest_dir
    )

    top_level_of_logical_dest_dir = dest_dir / logical_local_root.name
    shutil.rmtree(top_level_of_logical_dest_dir, ignore_errors=True)

    def copy_to(src: Path, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        copy(src, dest)

    for _ in parallel.failfast(
        parallel.yield_all(
            ((src, thunks.thunking(copy_to)(src, dest)) for src, dest in operations),
            executor_cm=executor_cm,
        )
    ):
        pass
    return top_level_of_logical_dest_dir


@dataclass
class SourceTree(os.PathLike):
    """Represent a fixed set of sources (with hashes where available) as a list of
    sources, plus the (optional) logical root of the tree, so that they can be 'unwrapped'
    as a local directory structure.
    """

    sources: ty.List[Source]
    higher_logical_root: str = ""
    # there may be cases where, rather than identifying the 'lowest common prefix' of a
    # set of sources/URIs, we may wish to represent a 'higher' root for the sake of some
    # consuming system.  in those cases, this can be specified and we'll find the lowest
    # common prefix _above_ that.

    def path(self, dest_dir: ty.Optional[types.StrOrPath] = None) -> Path:
        """Return a local path to a directory that corresponds to the logical root.

        This incurs a download of _all_ sources explicitly represented by the list.

        If you want to _ensure_ that _only_ the listed sources are present in the
        directory, despite any other files which may be present in an
        implementation-specific cache, you must pass a Path to a directory that you are
        willing to have emptied, and this method will copy the files into it.
        """
        with cm.keep_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_PARALLELISM)
        ) as thread_pool:
            local_paths = [
                local_path
                for _, local_path in parallel.failfast(
                    parallel.yield_all(
                        # src.path() is a thunk that downloads the data if not already present locally.
                        # Source allows registration of download handlers by URI scheme.
                        ((src, src.path) for src in self.sources),
                        executor_cm=thread_pool,
                    )
                )
            ]

            if len(local_paths) == 1:
                local_logical_root = local_paths[0].parent.resolve()
            else:
                local_logical_root = Path(
                    logical_root.find(map(str, local_paths), self.higher_logical_root)
                )
                assert local_logical_root.is_dir()

            if not dest_dir:
                return local_logical_root

            return replicate_logical_tree(
                local_paths, local_logical_root, Path(dest_dir).resolve(), executor_cm=thread_pool
            )

    def __fspath__(self) -> str:  # implement the os.PathLike protocol
        return str(self.path())
