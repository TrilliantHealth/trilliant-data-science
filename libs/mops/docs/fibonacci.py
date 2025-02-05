import argparse
import os

from thds.mops import pure


def _blob_root() -> str:
    return os.getenv("URI") or pure.magic.local_root()


@pure.magic(blob_root=_blob_root)
def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("n", type=int)
    parser.add_argument(
        "--blob-store-uri",
        default=_blob_root(),
        help="must be a URI with a scheme:// that is supported by a registered blob store",
    )
    parser.add_argument("--disable-mops", "-d", action="store_true")
    args = parser.parse_args()

    os.environ["URI"] = args.blob_store_uri
    # the environ thing is a bit of a hack, to make this work recursively across processes.

    n = args.n
    if args.disable_mops:
        pure.magic.off()

    print(f"fibonacci({n}) == {fibonacci(n)}")
