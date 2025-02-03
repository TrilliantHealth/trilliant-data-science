import argparse
from pathlib import Path

from thds.adls.cached_up_down import upload_through_cache
from thds.adls.uri import resolve_uri


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path, help="A local file you want to upload.")
    parser.add_argument("uri", type=resolve_uri, help="A fully qualified path to an ADLS location")
    args = parser.parse_args()

    upload_through_cache(args.uri, args.path)


if __name__ == "__main__":
    main()
