"""
Delete everything in a gent trash directory, in a process detached by `thds.gent.worktree_trash`.

Several `wt rm --trash` calls in a row each start a deleter, so only one at a time
does the work: the rest find the directory locked and exit, instead of walking the
same trees at once.
"""

import fcntl
import os
import shutil
import sys
from collections import abc
from datetime import datetime
from pathlib import Path


def log_path(trash: Path) -> Path:
    return trash.with_name(trash.name + ".log")


def _try_lock(trash: Path) -> int | None:
    fd = os.open(trash, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None

    return fd


def _remaining(trash: Path, failed: frozenset[Path]) -> list[Path]:
    return sorted(set(trash.iterdir()) - failed)


def _delete(entries: abc.Iterable[Path]) -> frozenset[Path]:
    """Delete each entry, returning the ones that could not be deleted."""
    failed: set[Path] = set()
    for entry in entries:
        try:
            shutil.rmtree(entry)
        except OSError as e:
            print(f"{datetime.now().isoformat()} failed to delete {entry}: {e}", file=sys.stderr)
            failed.add(entry)
    return frozenset(failed)


def empty(trash: Path) -> None:
    """Delete everything in the trash, or leave it to a process already doing so.

    The lock holder looks for new entries again after releasing the lock, so an
    entry added before this call is never left behind by both processes. An entry
    that fails to delete is logged and skipped until the next call.
    """
    failed: frozenset[Path] = frozenset()
    while _remaining(trash, failed):
        fd = _try_lock(trash)
        if fd is None:
            return

        try:
            while entries := _remaining(trash, failed):
                failed = failed | _delete(entries)
        finally:
            os.close(fd)  # releases the lock
