import argparse
from pathlib import Path

from thds.adls import abfss, fqn
from thds.adls.cached_up_down import download_to_cache
from thds.core.link import link


def parse(uri: str) -> fqn.AdlsFqn:
    """Works with ABFSS and adls schemes."""
    try:
        return fqn.AdlsFqn.parse(uri)
    except fqn.NotAdlsUri:
        pass
    return abfss.to_adls_fqn(uri)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", type=parse)
    parser.add_argument("--copy-to", "-c", type=Path)

    args = parser.parse_args()

    cache_path = download_to_cache(args.uri)
    if args.copy_to:
        link(cache_path, args.copy_to)
        print(args.copy_to.resolve())
    else:
        print(cache_path.resolve())
