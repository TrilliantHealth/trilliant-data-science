"""Part of the design of our lock is that a remote process can take over 'maintenance' of
the lock if (and especially) if the orchestrator process dies.

This allows a killed orchestrator process to be restarted as long as all of its remote
processes have gotten started working.

The remote process lock maintainers never _acquire_ the lock; they simply read what's in
it when they get started, and from then on keep the `written_at` timestamp up to date.

"""

import heapq
import os
import threading
import time
import typing as ty
from dataclasses import dataclass
from datetime import datetime, timedelta

from thds.core import cache, config, log, scope

from ._funcs import make_lock_uri
from .read import get_writer_id, make_read_lockfile
from .types import LockAcquired
from .write import LockEmitter, LockfileWriter

MAINTAIN_LOCKS = config.item("thds.mops.pure.local.maintain_locks", default=True, parse=config.tobool)
_MAINTENANCE_MARGIN = 0.5  # multiplier for the expire time
assert _MAINTENANCE_MARGIN < 1, "Maintenance margin must be less than 1 or locks will expire!"

_MAX_LOCKS_PER_THREAD = 200  # I want to leave lots of margin so that locks don't expire.

logger = log.getLogger(__name__)


class _LockMaintenanceKit(ty.NamedTuple):
    wakeup_time: float
    lock_acquired: LockAcquired
    should_exit: ty.Callable[[], bool]


class _LockMaintenanceThreadState(ty.NamedTuple):
    heap: list[_LockMaintenanceKit]
    heap_lock: threading.Lock
    lock_added_event: threading.Event


@scope.bound
def _maintenance_daemon(state: _LockMaintenanceThreadState, daemon_num: int) -> None:
    """Daemon thread that maintains a set of locks."""
    scope.enter(log.logger_context(pid=os.getpid(), maint_daemon_num=daemon_num))
    log_at_level = logger.warning if daemon_num > 0 else logger.debug
    log_at_level("Starting lock maintenance daemon thread %s", daemon_num)

    while True:
        with state.heap_lock:
            if not state.heap:
                next_wakeup_time = None
            else:
                next_wakeup_time = state.heap[0].wakeup_time

        if next_wakeup_time is None:
            logger.debug("No locks to maintain; waiting indefinitely for new ones")
            state.lock_added_event.wait()
            state.lock_added_event.clear()
            continue

        # Wait until either: next maintenance time OR new lock added
        sleep_duration = max(0, next_wakeup_time - time.monotonic())
        woke_early = state.lock_added_event.wait(timeout=sleep_duration)
        state.lock_added_event.clear()

        if woke_early:
            continue  # go back to the beginning and check for the highest priority lock

        # Time to do maintenance
        while state.heap and state.heap[0].wakeup_time <= time.monotonic():
            with state.heap_lock:
                _, lock_obj, should_exit_fn = heapq.heappop(state.heap)

            if not should_exit_fn():
                try:
                    logger.debug("Maintaining lock %s", lock_obj.writer_id)
                    lock_obj.maintain()
                    # Re-schedule for next maintenance
                    with state.heap_lock:
                        next_maintenance = time.monotonic() + (lock_obj.expire_s * _MAINTENANCE_MARGIN)
                        heapq.heappush(
                            state.heap,
                            _LockMaintenanceKit(next_maintenance, lock_obj, should_exit_fn),
                        )
                except Exception:
                    logger.exception(f"Failed to maintain lock: {lock_obj}")


@dataclass
class _ShouldExit:
    lock_acquired: LockAcquired
    should_exit: bool = False

    def check_status(self) -> bool:
        return self.should_exit

    def stop_maintaining(self) -> None:
        self.should_exit = True
        self.lock_acquired.release()


_LOCK_RELEASERS_BY_ID = dict[str, ty.Callable[[], None]]()
_LOCK_MAINTENANCE_DAEMON_STATES = dict[int, _LockMaintenanceThreadState]()


@cache.locking
def _ensure_daemon(thread_num: int) -> None:
    """Start the maintenance daemon exactly once."""
    lock_state = _LockMaintenanceThreadState(
        heap=[],
        heap_lock=threading.Lock(),
        lock_added_event=threading.Event(),
    )
    assert thread_num not in _LOCK_MAINTENANCE_DAEMON_STATES  # protected by the cache.locking decorator
    _LOCK_MAINTENANCE_DAEMON_STATES[thread_num] = lock_state
    threading.Thread(target=_maintenance_daemon, args=(lock_state, thread_num), daemon=True).start()


def add_lock_to_maintenance_daemon(lock_acq: LockAcquired) -> ty.Callable[[], None]:
    """Add lock to global maintenance system and return a cleanup function."""
    if lock_acq.writer_id in _LOCK_RELEASERS_BY_ID:
        # technically we could be locking this, but mops itself does not allow
        # multiple callers to ask for the same lock to be maintained at the same time;
        # it will always be either the runner or the future that the runner has created.
        return _LOCK_RELEASERS_BY_ID[lock_acq.writer_id]

    should_exit = _ShouldExit(lock_acq)

    for i in range(len(_LOCK_MAINTENANCE_DAEMON_STATES) + 1):
        maintenance_daemon_state = _LOCK_MAINTENANCE_DAEMON_STATES.get(i)
        if maintenance_daemon_state is None:
            _ensure_daemon(i)
            maintenance_daemon_state = _LOCK_MAINTENANCE_DAEMON_STATES[i]
        elif len(maintenance_daemon_state.heap) > _MAX_LOCKS_PER_THREAD:
            continue  # go to next thread if this one is too full

        with maintenance_daemon_state.heap_lock:
            next_time = time.monotonic() + (lock_acq.expire_s * _MAINTENANCE_MARGIN)
            heapq.heappush(
                maintenance_daemon_state.heap,
                _LockMaintenanceKit(next_time, lock_acq, should_exit.check_status),
            )
        maintenance_daemon_state.lock_added_event.set()
        break  # we found a thread that can take the lock

    _LOCK_RELEASERS_BY_ID[lock_acq.writer_id] = should_exit.stop_maintaining
    return should_exit.stop_maintaining


# from this point down, the code is about how to prepare to call add_lock_to_maintenance_daemon
# from the remote side, and what happens if the lock cannot or should not be maintained.


class CannotMaintainLock(ValueError):
    pass  # pragma: no cover


class LockWasStolenError(ValueError):
    pass  # pragma: no cover


def make_remote_lock_writer(lock_dir_uri: str, expected_writer_id: str = "") -> LockAcquired:
    """Only for use by remote side - does not _acquire_ the lock,
    but merely allows for it to be maintained as unexpired. Does not allow for releasing,
    as it is not the responsibility of the remote side to release the lock.

    Will raise a CannotMaintainLock exception if the lock does not exist or has no
    expiration time.

    Will raise a LockWasStolenError if a provided expected_writer_id (which is the
    writer_id of the lock as provided to the remote side by the original writer) does not
    match the lock's actual current writer_id - in other words, if some other writer has
    acquired the lock before the remote side has been able to start running.

    Notably, this is a race condition! The remote side depends on actual lock holders to
    cooperate in having only a single lock holder; the remote is simply checking a single
    time and then maintaining the lock indefinitely if the writer_id matches.

    TODO: If the lock is already expired but the writer_id still matches, perhaps we
    could be acquiring the lock to eliminate the race, and if we fail, we would
    exit with LockWasStolenError...

    The return value is intended to be passed to add_lock_to_maintenance_daemon.
    """
    try:
        lock_uri = make_lock_uri(lock_dir_uri)
        read_lockfile = make_read_lockfile(lock_uri)
        lock_contents = read_lockfile()
    except Exception as exc:
        raise CannotMaintainLock(f"Could not read lockfile in lock dir: {lock_dir_uri}") from exc

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
        LockEmitter(get_writer_id(lock_contents), timedelta(seconds=expire_s)),
        expire_s,
        writer_name="remote",
    )
    lockfile_writer.first_acquired_at = datetime.fromisoformat(first_acquired_at_s)
    # disable releasing from remote
    lockfile_writer.release = lambda: None  # type: ignore # noqa: E731
    return lockfile_writer


def maintain_to_release(acquired_lock: LockAcquired) -> ty.Callable[[], None]:
    """Depending on configuration, potentially start maintaining the lock.

    Return a callable that will release the lock when called.
    """
    if MAINTAIN_LOCKS():
        return add_lock_to_maintenance_daemon(acquired_lock)

    return acquired_lock.release


def no_maintain() -> None:
    MAINTAIN_LOCKS.set_global(False)
