import argparse
from pathlib import Path

from thds.adls import fqn
from thds.adls.cached_up_down import download_to_cache
from thds.adls.link import link


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", type=fqn.parse)
    parser.add_argument("--copy-to", "-c", type=Path)

    args = parser.parse_args()

    cache_path = download_to_cache(args.uri)
    if args.copy_to:
        link(cache_path, args.copy_to)
