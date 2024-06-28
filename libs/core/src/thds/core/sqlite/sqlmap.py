import shutil
import typing as ty
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from uuid import uuid4

from thds.core import log, parallel, scope, thunks, tmp

from .merge import merge_databases

logger = log.getLogger(__name__)
_tmpdir_scope = scope.Scope()


class Partition(ty.NamedTuple):
    partition: int  # 0 indexed
    of: int  # count (1 indexed)


ALL = Partition(0, 1)


def _name(callable: ty.Callable) -> str:
    if hasattr(callable, "func"):
        return callable.func.__name__
    return callable.__name__


def _write_partition(
    basename: str, writer: ty.Callable[[Partition, Path], ty.Any], base_dir: Path, partition: Partition
) -> Path:
    part_dir = base_dir / f"-{basename}-{partition.partition:02d}of{partition.of:02d}"
    part_dir.mkdir(exist_ok=True, parents=True)
    with log.logger_context(p=f"{partition.partition + 1:2d}/{partition.of:2d}"):
        logger.info(f"Writing partition '{partition}' outputs into {part_dir}")
        try:
            writer(partition, part_dir)
        except Exception:
            logger.exception(f"Failed to write partition '{partition}' outputs into {part_dir}")
            raise
        logger.info(f"Finished writing partition {partition} outputs into {part_dir}")
        return part_dir


def merge_sqlite_dirs(
    part_dirs: ty.Iterable[Path], output_dir: Path, max_cores: int = 2
) -> ty.Dict[str, Path]:
    """Any file found in any of these directories is assumed to be a SQLite database, and
    will be merged into all the other SQLite databases that bear the same name in any of
    the other directories.

    The final, merged SQLite database will then be _moved_ into the output_dir provided,
    keeping the same same.
    """
    sqlite_dbs_by_filename: ty.Dict[str, ty.List[Path]] = defaultdict(list)
    for partition_dir in part_dirs:
        if not partition_dir.exists():
            continue  # a partition writer is allowed to write out nothing to a partition
        if not partition_dir.is_dir():
            # this may happen if people don't read the parallel_to_sqlite docstring and
            # assume the Path is meant to be a file.
            raise ValueError(
                f"Partition directory {partition_dir} is not a directory!"
                " Your code may have written directly to the provided Path as a file,"
                " rather than writing SQLite database files into the directory as required."
            )
        for sqlite_db_path in partition_dir.iterdir():
            if sqlite_db_path.is_file():
                sqlite_dbs_by_filename[sqlite_db_path.name].append(sqlite_db_path)

    for merged_db in parallel.yield_results(
        [
            thunks.thunking(merge_databases)(sqlite_db_paths)
            for filename, sqlite_db_paths in sqlite_dbs_by_filename.items()
        ],
        # SQLite merge is CPU-intensive, so we use a Process Pool.
        executor_cm=ProcessPoolExecutor(max_workers=max(min(max_cores, len(sqlite_dbs_by_filename)), 1)),
    ):
        logger.info(f"Moving merged database {merged_db} into {output_dir}")
        shutil.move(str(merged_db), output_dir)

    return {filename: output_dir / filename for filename in sqlite_dbs_by_filename}


def _ensure_output_dir(output_directory: Path):
    if output_directory.exists():
        if not output_directory.is_dir():
            raise ValueError("Output path must be a directory if it exists!")
    else:
        output_directory.mkdir(parents=True)
    assert output_directory.is_dir()


@_tmpdir_scope.bound
def parallel_to_sqlite(
    partition_writer: ty.Callable[[Partition, Path], ty.Any],
    output_directory: Path,
    N: int = 8,
) -> ty.Dict[str, Path]:
    """The partition_writer will be provided a partition number and a directory (as a Path).

    It must write one or more sqlite databases to the directory provided, using (of
    course) the given partition to filter/query its input.

    Any files found in that output directory will be assumed to be a SQLite database, and
    any _matching_ filenames across the set of partitions that are written will be merged
    into each other. Therefore, there will be a single database in the output directory
    for every unique filename found in any of the partition directories after writing.
    """
    _ensure_output_dir(output_directory)
    temp_dir = _tmpdir_scope.enter(tmp.tempdir_same_fs(output_directory))

    part_directories = list(
        parallel.yield_results(
            [
                thunks.thunking(_write_partition)(
                    _name(partition_writer) + uuid4().hex[:20],
                    partition_writer,
                    temp_dir,
                    Partition(i, N),
                )
                for i in range(N)
            ],
            executor_cm=ProcessPoolExecutor(max_workers=N),
            # executor_cm=contextlib.nullcontext(loky.get_reusable_executor(max_workers=N)),
        )
    )
    return merge_sqlite_dirs(part_directories, output_directory, max_cores=N)
