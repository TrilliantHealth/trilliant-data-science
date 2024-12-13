"""A program that exits after a given number of seconds.

but whose exit can be delayed by modifying a file on the filesystem.
"""

import argparse
import os
import time
import typing as ty
from datetime import datetime, timedelta, timezone
from logging import getLogger
from pathlib import Path

logger = getLogger(__name__)


def now() -> datetime:
    return datetime.now(tz=timezone.utc)


def report(exit_time: datetime) -> datetime:
    reporting_at = now()
    msg = f"Will exit at {exit_time}, in {exit_time - reporting_at}"
    logger.info(msg)
    return reporting_at


def try_parse(time_file: Path, default: datetime) -> datetime:
    dt_s = ""
    try:
        with open(time_file) as f:
            dt_s = f.read().strip()
            dt = datetime.fromisoformat(dt_s)
            if default != dt:
                logger.info(f"Parsed new time {dt} from {time_file}")
            return dt
    except FileNotFoundError:
        logger.debug(f"No file found at {time_file}")
    except ValueError:
        logger.exception(f"Unable to parse {time_file} with contents {dt_s}; using default {default}")
    return default


_1_SECOND = timedelta(seconds=1)


def exit_when(
    exit_time_file: Path,
    exit_at: datetime,
    *,
    report_every: ty.Optional[timedelta],
    check_every: timedelta = _1_SECOND,
    keep_existing_file: bool = False,
) -> timedelta:
    # setup
    started = now()
    exit_time_file = exit_time_file.resolve()
    if not exit_at.tzinfo:
        exit_at = exit_at.astimezone(timezone.utc)

    if not keep_existing_file:
        with open(exit_time_file, "w") as f:
            f.write(exit_at.isoformat())

    exit_at = try_parse(exit_time_file, exit_at)
    if report_every:
        reported_at = report(exit_at)
    while True:
        rem = (exit_at - now()).total_seconds()
        if rem <= 0:
            break
        time.sleep(min(check_every.total_seconds(), rem))
        exit_at = try_parse(exit_time_file, exit_at)
        if report_every and now() - reported_at > report_every:
            reported_at = report(exit_at)

    os.unlink(exit_time_file)
    return now() - started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--exit-time-file",
        "-f",
        default="~/WILL_EXIT_AT_THIS_TIME.txt",
        help=(
            "Modify this file while the program is running to accelerate or delay the exit."
            " Will be deleted and recreated if it exists, unless --keep-existing-file is also passed."
        ),
    )
    parser.add_argument(
        "--seconds-from-now",
        "-s",
        default=timedelta(hours=1).total_seconds(),
        type=float,
    )
    parser.add_argument(
        "--report-every-s",
        "-r",
        default=timedelta(minutes=20).total_seconds(),
        type=float,
        help="Report time of exit this often (in seconds)",
    )
    parser.add_argument(
        "--keep-existing-file",
        action="store_true",
        help="Unless set, an existing file will be presumed to be from a previous run",
    )
    args = parser.parse_args()
    elapsed = exit_when(
        Path(os.path.expanduser(args.exit_time_file)),
        datetime.now(tz=timezone.utc) + timedelta(seconds=args.seconds_from_now),
        report_every=timedelta(seconds=args.report_every_s),
        keep_existing_file=args.keep_existing_file,
    )
    logger.info(f"Exiting after {elapsed}")


if __name__ == "__main__":
    main()
