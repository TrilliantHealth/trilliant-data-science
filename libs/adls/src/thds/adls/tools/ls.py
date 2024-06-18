import argparse

from thds.adls import ADLSFileSystem
from thds.adls.file_properties import get_file_properties, is_directory
from thds.adls.uri import resolve_uri


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", type=resolve_uri, help="A fully qualified path to an ADLS location")
    parser.add_argument(
        "--verbose",
        "-v",
        default=False,
        action="store_true",
        help="Verbose mode will display ADLS metadata associated with contents of the specified URI",
    )

    args = parser.parse_args()

    fs = ADLSFileSystem(args.uri.sa, args.uri.container)

    files_info = (
        fs.get_directory_info(args.uri.path)
        if is_directory(get_file_properties(args.uri))
        else fs.get_files_info([args.uri.path])
    )
    for f in files_info:
        if args.verbose:
            print(f["name"])
            [print(f"    {key}: {value}") for key, value in sorted(f.items()) if key != "name"]
            print("\n")
        else:
            print(f["name"])


if __name__ == "__main__":
    main()
