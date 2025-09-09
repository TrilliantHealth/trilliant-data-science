import argparse

from thds.adls import list_fast, uri


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", type=uri.parse_any, help="A fully qualified path to an ADLS location")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    for f in list_fast.multilayer_yield_blob_meta(args.uri):
        print(f.path)
        if args.verbose:
            print("      ", f.size)
            print("      ", f.hash)
            print("      ", f.metadata)
            print()


if __name__ == "__main__":
    main()
