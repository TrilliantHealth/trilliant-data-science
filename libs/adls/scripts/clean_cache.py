#!/usr/bin/env python
"""Clean your local cache of things that no longer exist remotely."""

import argparse
import os
import typing as ty
from multiprocessing import Pool
from pathlib import Path

from thds.adls import fqn
from thds.adls.global_client import get_global_fs_client
from thds.adls.ro_cache import global_cache


def test_and_clean(path: Path, fqn: fqn.AdlsFqn):
    gc = get_global_fs_client(fqn.sa, fqn.container)
    # according to
    # https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview#move-data-based-on-last-accessed-time
    # checking if the file exists does not count as an access for the
    # purposes of resetting the lastAccessTime, which would then keep
    # a file alive under certain lifecycle management policies.
    if not gc.get_file_client(fqn.path).exists():
        try:
            path.unlink()
            print(f"Removed {path}")
        except Exception as e:
            return e, path
    # TODO: optionally, support md5ing locally and removing if it doesn't match.
    return None


def _test_and_clean(t):
    return test_and_clean(*t)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=str(global_cache().root),
        type=Path,
        help="Root of cache, defaults to {_GLOBAL}",
    )

    args = parser.parse_args()

    def get_fqn(file: Path) -> ty.Optional[fqn.AdlsFqn]:
        try:
            uri = "adls://" + str(file)[len(str(args.root)) + 1 :]
            return fqn.AdlsFqn.parse(uri)
        except fqn.NotAdlsUri:
            return None

    paths_that_are_fqns = [
        (path, fqn) for path, fqn in ((path, get_fqn(path)) for path in args.root.glob("**/*")) if fqn
    ]

    with Pool(40) as pool:
        failures = pool.map(_test_and_clean, paths_that_are_fqns)

    for exc, path in filter(None, failures):  # type: ignore
        print(f"Failed to delete {path} b/c of {exc}")  # type: ignore

    for root, dirs, _files in os.walk(args.root, topdown=False):
        for name in dirs:
            dirpath = Path(root) / name
            if not any(dirpath.iterdir()):
                print(f"Removing empty directory {dirpath}")
                dirpath.rmdir()


if __name__ == "__main__":
    main()
