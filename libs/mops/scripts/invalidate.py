#!/usr/bin/env python
import argparse
from pathlib import Path

from thds import adls
from thds.mops.pure.adls.blob_store import _DEFAULT_CONTROL_CACHE, DangerouslyCachingStore

store = DangerouslyCachingStore(_DEFAULT_CONTROL_CACHE())


# delete the file at the given URI
# also delete the


def invalidate_cache(mops_fqn: adls.AdlsFqn) -> None:
    if not mops_fqn.path.endswith("result"):
        mops_fqn = mops_fqn / "result"

    local_path = store._cache.path(mops_fqn)
    if local_path.exists():
        local_path.unlink()
        print(f"Deleted local cache at {local_path}")
    else:
        print(f"Local cache path {local_path} does not exist, skipping deletion.")

    fs = adls.ADLSFileSystem(mops_fqn.sa, mops_fqn.container)
    fs.delete_file(mops_fqn.path)
    print(f"Deleted {mops_fqn}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--uri",
        "-u",
        type=adls.uri.resolve_uri,
        help="A fully qualified path to an ADLS location. Accepts adls://, https:// and abfss:// URIs.",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=Path,
        help="A file containing a list of URIs to invalidate, one per line.",
    )
    args = parser.parse_args()

    if args.uri:
        invalidate_cache(args.uri)
    if args.file:
        for uri in args.file.read_text().splitlines():
            invalidate_cache(adls.uri.parse_uri(uri))


if __name__ == "__main__":
    main()
