"""Part of the design of our lock is that a remote process can take over 'maintenance' of
the lock if (and especially) if the orchestrator process dies.

This allows a killed orchestrator process to be restarted as long as all of its remote
processes have gotten started working.

The remote process lock maintainers never _acquire_ the lock; they simply read what's in
it when they get started, and from then on keep the `written_at` timestamp up to date.

"""

import time
import typing as ty
from datetime import datetime, timedelta
from functools import partial
from threading import Thread

from ._lock import LockfileWriter, make_lock_contents, make_lock_uri, make_read_lockfile


class _Maintain(ty.NamedTuple):
    maintain: ty.Callable[[], None]
    expire_s: float


class _MaintainForever(ty.Protocol):
    def __call__(self) -> None:
        ...  # pragma: no cover


def _maintain_forever(maintain: ty.Callable[[], ty.Any], expire_s: float) -> None:
    while True:
        maintain()
        # maintain the lock twice as often as necessary, to be safe
        time.sleep(expire_s / 2)


def remote_lock_maintain(lock_dir_uri: str) -> _Maintain:
    """Only for use by remote side - does not _acquire_ the lock,
    but merely maintains it as unexpired. Does not allow for releasing,
    as it is not the responsibility of the remote side to release the lock.

    The return value is intended to be launched as the target of a Thread or Process.
    """
    lock_uri = make_lock_uri(lock_dir_uri)
    read_lockfile = make_read_lockfile(lock_uri)

    lock_contents = read_lockfile()
    if not lock_contents:
        raise ValueError(f"Should not be maintaining a lock that does not exist: {lock_uri}")

    expire_s = lock_contents["expire_s"]
    if not expire_s or expire_s < 0:
        raise ValueError(f"Lockfile is missing an expiry time: {lock_contents}")

    first_acquired_at_s = lock_contents["first_acquired_at"]
    if not first_acquired_at_s:
        raise ValueError(f"Should not be maintaining a lock that was never acquired: {lock_contents}")

    lockfile_writer = LockfileWriter(
        lock_dir_uri, make_lock_contents(lock_uri, timedelta(seconds=expire_s))
    )
    lockfile_writer.first_acquired_at = datetime.fromisoformat(first_acquired_at_s)

    return _Maintain(lockfile_writer.maintain, expire_s)


def launch_daemon_lock_maintainer(lock_dir_uri: str):
    """Run lock maintenance until the process exits."""
    maintain, expire_s = remote_lock_maintain(lock_dir_uri)
    Thread(target=partial(_maintain_forever, maintain, expire_s), daemon=True).start()
