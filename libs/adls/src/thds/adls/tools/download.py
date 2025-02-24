import argparse
from pathlib import Path

from thds.adls.cached_up_down import download_directory, download_to_cache
from thds.adls.file_properties import get_file_properties, is_directory
from thds.adls.uri import resolve_uri
from thds.core.link import link


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", type=resolve_uri, help="A fully qualified path to an ADLS location")
    parser.add_argument(
        "--copy-to",
        "-c",
        type=Path,
        help="This will create a link to the cached download at the specified location",
    )

    args = parser.parse_args()

    if is_directory(get_file_properties(args.uri)):
        cache_path = download_directory(args.uri)
    else:
        cache_path = download_to_cache(args.uri)

    if args.copy_to:
        link(cache_path, args.copy_to)
        print(args.copy_to.resolve())
    else:
        print(cache_path.resolve())


if __name__ == "__main__":
    main()
