import argparse
from pathlib import Path

from thds.adls.cached_up_down import download_to_cache
from thds.adls.uri import resolve_uri
from thds.core.link import link


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", type=resolve_uri)
    parser.add_argument("--copy-to", "-c", type=Path)

    args = parser.parse_args()

    cache_path = download_to_cache(args.uri)
    if args.copy_to:
        link(cache_path, args.copy_to)
        print(args.copy_to.resolve())
    else:
        print(cache_path.resolve())


if __name__ == "__main__":
    main()
