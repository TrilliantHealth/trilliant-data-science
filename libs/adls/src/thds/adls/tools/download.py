import argparse
from pathlib import Path

from thds.adls.cached_up_down import download_directory, download_to_cache
from thds.adls.file_properties import get_file_properties, is_directory
from thds.adls.impl import ADLSFileSystem
from thds.adls.uri import resolve_uri
from thds.core.link import link


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "adls_fqn",
        type=resolve_uri,
        help="A fully qualified path to an ADLS location. Accepts adls://, https:// and abfss:// URIs.",
    )
    parser.add_argument(
        "--copy-to",
        "-c",
        type=Path,
        help="This will create a link to the cached download at the specified location",
    )
    parser.add_argument(
        "--use-async",
        action="store_true",
        help="Only useful if you want to exercise the async code; does not make things faster or better.",
    )

    args = parser.parse_args()

    is_dir = is_directory(get_file_properties(args.adls_fqn))

    if args.use_async:
        fs = ADLSFileSystem(args.adls_fqn.sa, args.adls_fqn.container)
        if is_dir:
            cache_path = fs.fetch_directory(args.adls_fqn.path)[0]
        else:
            cache_path = fs.fetch_file(args.adls_fqn.path)
    else:
        if is_dir:
            cache_path = download_directory(args.adls_fqn)
        else:
            cache_path = download_to_cache(args.adls_fqn)

    if args.copy_to:
        link(cache_path, args.copy_to)
        print(args.copy_to.resolve())
    else:
        print(cache_path.resolve())


if __name__ == "__main__":
    main()
