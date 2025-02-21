import argparse
import time
import typing as ty
from pathlib import Path

from . import _acquire


def _writer(out_times_path: Path) -> ty.Callable[[float, float], None]:
    _acquire.logger.info(f"Will write Lock Times to {out_times_path}")
    out_times_path.parent.mkdir(parents=True, exist_ok=True)

    def write_times(after_acquired: float, before_released: float) -> None:
        _acquire.logger.info(f"..........appending {after_acquired},{before_released}")
        with out_times_path.open("a") as f:
            f.write(f"{after_acquired},{before_released}\n")

    return write_times


def acquire_and_hold_once(
    lock_uri: str, hold_once_acquired_s: float, out_times_path: ty.Optional[Path]
) -> None:
    if out_times_path:
        write_times = _writer(out_times_path)
    else:
        write_times = lambda x, y: None  # noqa: E731

    # we want more verbose logging when using the CLI.
    _acquire.logger.debug = _acquire.logger.info  # type: ignore

    _acquire.logger.info(f"Beginning lock acquisition on {lock_uri}")
    lock_owned = _acquire.acquire(lock_uri, block=None)
    assert lock_owned
    when_lock_acquired = time.time()
    # we're using time, not timeit.default_timer, because we care about time
    # differences between multiple processes on the same system, so we can compare
    # them afterward.
    time_until_release = when_lock_acquired + hold_once_acquired_s - time.time()
    while time_until_release > 0:
        lock_owned.maintain()
        time.sleep(min(time_until_release, 4))
        # don't wake up to maintain a lot - every 4 seconds is enough for the default 30s expiry.
        time_until_release = when_lock_acquired + hold_once_acquired_s - time.time()

    write_times(when_lock_acquired, time.time())
    lock_owned.release()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lock_uri", help="URI to the lockfile")
    parser.add_argument(
        "--hold-once-acquired-s",
        "-t",
        type=float,
        default=20.0,
        help="Time in seconds to hold the lock once acquired.",
    )
    parser.add_argument(
        "--out-times",
        type=Path,
        default=None,
        help="Write out the periods of time the lock was fully held (after acquire, before release) to this file.",
    )

    args = parser.parse_args()

    acquire_and_hold_once(args.lock_uri, args.hold_once_acquired_s, args.out_times)


if __name__ == "__main__":
    main()
