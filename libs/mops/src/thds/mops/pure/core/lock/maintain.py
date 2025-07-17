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

from thds.core import log

from ._funcs import make_lock_uri
from .read import get_writer_id, make_read_lockfile
from .types import LockAcquired
from .write import LockfileWriter, make_lock_contents

logger = log.getLogger(__name__)


class _MaintainOnly(ty.NamedTuple):
    """Matches the LockAcquired interface except that release() will do nothing."""

    maintain: ty.Callable[[], None]
    expire_s: float
    release: ty.Callable[[], None]


class _MaintainForever(ty.Protocol):
    def __call__(self) -> None:
        ...  # pragma: no cover


def _maintain_forever(
    maintain: ty.Callable[[], ty.Any], expire_s: float, should_exit: ty.Callable[[], bool]
) -> None:
    while True:
        # maintain the lock twice as often as necessary, to be safe
        time.sleep(expire_s / 2)
        if should_exit():
            return
        maintain()


class CannotMaintainLock(ValueError):
    pass  # pragma: no cover


class LockWasStolenError(ValueError):
    pass  # pragma: no cover


def remote_lock_maintain(lock_dir_uri: str, expected_writer_id: str = "") -> LockAcquired:
    """Only for use by remote side - does not _acquire_ the lock,
    but merely maintains it as unexpired. Does not allow for releasing,
    as it is not the responsibility of the remote side to release the lock.

    Will raise a CannotMaintainLock exception if the lock does not exist or has no
    expiration time.

    Will raise a LockWasStolenError if a provided expected_writer_id (which is the
    writer_id of the lock as provided to the remote side by the original writer) does not
    match the lock's actual current writer_id - in other words, if some other writer has
    acquired the lock before the remote side has been able to start running.

    The return value is intended to be launched as the target of a Thread or Process.
    """

    try:
        lock_uri = make_lock_uri(lock_dir_uri)
        read_lockfile = make_read_lockfile(lock_uri)
        lock_contents = read_lockfile()
    except Exception:
        logger.exception(f"Could not read lockfile: {lock_uri}")

    if not lock_contents:
        raise CannotMaintainLock(f"Lock does not exist: {lock_uri}")

    expire_s = lock_contents["expire_s"]
    if not expire_s or expire_s < 0:
        raise CannotMaintainLock(f"Lock is missing an expiry time: {lock_contents}")

    first_acquired_at_s = lock_contents["first_acquired_at"]
    if not first_acquired_at_s:
        raise CannotMaintainLock(f"Lock was never acquired: {lock_contents}")

    current_writer_id = lock_contents["writer_id"]
    if expected_writer_id and expected_writer_id != current_writer_id:
        raise LockWasStolenError(
            "Refusing to maintain lock that was created by a different writer:"
            f" expected `{expected_writer_id}`, got `{current_writer_id}`."
            "This probably means you just need to kill and restart your orchestrator "
            " and it will begin awaiting the results of the new owner of the lock."
        )

    lockfile_writer = LockfileWriter(
        current_writer_id,
        lock_dir_uri,
        make_lock_contents(get_writer_id(lock_contents), timedelta(seconds=expire_s)),
        expire_s,
        writer_name="remote",
    )
    lockfile_writer.first_acquired_at = datetime.fromisoformat(first_acquired_at_s)
    # disable releasing from remote
    lockfile_writer.release = lambda: None  # type: ignore # noqa: E731
    return lockfile_writer


def launch_daemon_lock_maintainer(lock_acq: LockAcquired) -> ty.Callable[[], None]:
    """Run lock maintenance until the process exits, or until the returned callable gets
    returned.

    Return a 'release wrapper' that stops maintenance of the lock and releases it.

    A whole thread for this seems expensive, but the simplest alternative is having too
    many lock maintainers trying to share time slices within some global lock maintainer,
    and that runs a definite risk of overrunning the expiry time(s) for those locks.

    If we were async all the way down, we could more plausibly make a bunch of async
    network/filesystem calls here without taking into consideration how long they actually
    take to execute.
    """
    should_exit = False

    def should_stop_maintaining() -> bool:
        return should_exit

    Thread(
        target=partial(
            _maintain_forever,
            lock_acq.maintain,
            lock_acq.expire_s,
            should_stop_maintaining,
        ),
        daemon=True,
    ).start()

    def stop_maintaining() -> None:
        nonlocal should_exit
        should_exit = True
        lock_acq.release()

    return stop_maintaining
