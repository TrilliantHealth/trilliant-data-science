import argparse
import os

from thds.mops import pure


def _get_uri() -> str:
    return os.getenv("URI", "file://.")


@pure.memoize_in(_get_uri)
def fibonacci(n: int) -> int:
    """pipeline-id-mask: test"""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("n", type=int)
    parser.add_argument("--blob-store-uri", default=_get_uri())
    args = parser.parse_args()

    os.environ["URI"] = args.blob_store_uri
    # the os thing is a bit of a hack, to make this work recursively across processes.

    n = args.n
    print(f"fibonacci({n}) == {fibonacci(n)}")
