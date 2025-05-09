#!/usr/bin/env python3
import argparse
import os
import random
import secrets
import tempfile
import time
from pathlib import Path

from thds.core import log, source
from thds.mops import pure

_LOGGER = log.getLogger(__name__)
BYTES_PER_MB = 1024 * 1024
DEFAULT_MB = 16


@pure.magic("samethread")
def random_bytes(seed: int, mb: int) -> source.Source:
    _LOGGER.info("Generating %d MB of random bytes with seed %d", mb, seed)
    random.seed(seed)
    path = Path(tempfile.mkdtemp()) / "random_bytes"
    _LOGGER.info("Writing %d MB of random bytes to %s", mb, path)
    with open(path, "wb") as f:
        for _ in range(mb):
            # 1 MB of random bytes
            f.write(random.randbytes(BYTES_PER_MB))
        return source.from_file(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate random bytes and upload to ADLS as a side effect of a memoized `mops` computation."
    )
    parser.add_argument(
        "mb",
        type=int,
        default=DEFAULT_MB,
        required=False,
        help=f"The number of megabytes of random data to generate. (default: {DEFAULT_MB})",
    )
    args = parser.parse_args()
    mb = args.mb
    assert isinstance(mb, int)
    assert mb > 0, "Number of megabytes must be positive."

    seed = int.from_bytes(secrets.token_bytes(4), "big")
    # Generate a random seed from system entropy - as close as possible to truly random; we want to avoid cache hits
    # for this test
    tic = time.perf_counter()
    src = random_bytes(seed, mb)
    toc = time.perf_counter()
    runtime = toc - tic

    print(
        f"Generated {os.stat(src).st_size} random bytes at {src.path()} and memoized to ADLS in {runtime:.3f}s."
    )
