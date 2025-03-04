import argparse
import os

from thds.mops import pure


def _blob_root() -> str:
    return os.getenv("URI") or ""


@pure.magic(blob_root=_blob_root)
def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("n", type=int)
    parser.add_argument(
        "--blob-root",
        default=_blob_root() or fibonacci._get_blob_root(),
        help="must be a URI with a scheme:// that is supported by a registered blob store",
    )
    parser.add_argument("--disable-mops", "-d", action="store_true")
    args = parser.parse_args()

    pure.magic.load_config_file()

    os.environ["URI"] = args.blob_root
    # the environ thing is a bit of a hack, to make this work recursively across processes.

    n = args.n
    if args.disable_mops:
        pure.magic.off()

    print(f"fibonacci({n}) == {fibonacci(n)}")
