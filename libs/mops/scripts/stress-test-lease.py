#!/usr/bin/env python

# Loop thds.mops.pure.core.lease.cli.acquire_once N times in parallel with the same
# parameters, but different paths for --out-times, for M minutes.  Then, read all the
# output files (one per parallel run), and verify that none of those leased-time windows
# overlap (which would be a violation of the lease).
#
# The output time windows are CSV files with the following columns:
# after_acquire: a unix epoch time immediately after acquiring the lease
# before_release: a unix epoch time immediately after releasing the lease
import argparse
import concurrent.futures
import re
import threading
import time
import typing as ty
from functools import partial
from pathlib import Path
from timeit import default_timer
from uuid import uuid4

from thds.core import log
from thds.mops.pure.core.lease.cli import acquire_and_hold_once


def loop_1_for_m(idx: int, acquire_once: ty.Callable[[], None], minutes: float):
    start = default_timer()
    with log.logger_context(idx=f"{idx:03d}"):
        while default_timer() - start < minutes * 60:
            acquire_once()
            time.sleep(2)  # once we've gotten it, take a break and let somebody else get it.


def loop_n_for_m(lease_uri: str, times_dir: Path, hold_once_acquired_s: float, n: int, minutes: float):
    assert times_dir.is_dir() or not times_dir.exists(), f"{times_dir} exists and is not a directory"
    times_dir.mkdir(exist_ok=True, parents=True)

    with concurrent.futures.ProcessPoolExecutor(n) as ex:
        futures = [
            ex.submit(
                loop_1_for_m,
                i,
                acquire_once=partial(
                    acquire_and_hold_once,
                    lease_uri,
                    hold_once_acquired_s,
                    times_dir / f"lease-times-{i}",
                ),
                minutes=minutes,
            )
            for i in range(n)
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()


class LeaseTimes(ty.NamedTuple):
    after_acquire: float
    before_release: float
    idx: int


def _error_on_first_overlapping_interval(times_tuples: ty.List[LeaseTimes]) -> ty.Tuple[float, float]:
    times_tuples.sort(key=lambda x: x.after_acquire)  # sort by start time

    smallest_gap = 100000.0
    biggest_gap = 0.0
    for i in range(len(times_tuples)):
        if i == 0:
            continue

        gap = times_tuples[i].after_acquire - times_tuples[i - 1].before_release
        if gap < 0:
            raise ValueError(f"Overlap between {times_tuples[i - 1]} and {times_tuples[i]}")
        smallest_gap = min(smallest_gap, gap)
        biggest_gap = max(biggest_gap, gap)

    return smallest_gap, biggest_gap


def validate_leasefiles(times_dir: Path):
    times_files = list(times_dir.glob("lease-times-*"))

    times_tuples = []
    for lease_times_file in times_files:
        # get index from end of filename
        idx = int(re.search(r"\d+$", lease_times_file.stem).group(0))  # type: ignore

        for line in open(lease_times_file).read().splitlines():
            after_acquire, before_release = line.strip().split(",")
            assert after_acquire < before_release, (
                f"after_acquire {after_acquire} >= before_release {before_release}"
            )
            times_tuples.append(LeaseTimes(float(after_acquire), float(before_release), idx))

    if times_tuples:
        smallest_gap, biggest_gap = _error_on_first_overlapping_interval(times_tuples)
        log.getLogger(__name__).info(
            f"Checked {len(times_tuples)} times tuples and none of them overlap!"
            f" Smallest gap: {smallest_gap}, biggest gap: {biggest_gap}"
        )


def repeatedly_validate_leasefiles(times_dir: Path):
    while True:
        time.sleep(5)  # check every 5 seconds until process exit.
        validate_leasefiles(times_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("lease_uri")
    parser.add_argument("n", type=int)
    parser.add_argument(
        "--minutes", type=float, default=100.0, help="Test for a long time or a short one?"
    )
    parser.add_argument(
        "--hold-once-acquired-s", type=float, default=5.0, help="How often the leases should 'cycle'"
    )
    parser.add_argument(
        "--times-dir",
        type=Path,
        default=Path(f".lease-times-{uuid4().hex}"),
        help="Where to write the lease times",
    )

    args = parser.parse_args()

    if args.times_dir.exists():
        validate_leasefiles(args.times_dir)
        return

    threading.Thread(target=repeatedly_validate_leasefiles, args=(args.times_dir,), daemon=True).start()
    # keep validating these times files at all times - if a lease breaks, we want to know about it immediately.

    loop_n_for_m(args.lease_uri, args.times_dir, args.hold_once_acquired_s, args.n, args.minutes)
    validate_leasefiles(args.times_dir)


if __name__ == "__main__":
    main()
