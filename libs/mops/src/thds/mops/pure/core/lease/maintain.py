"""Part of the design of our lease is that a remote process can take over 'maintenance' of
the lease if (and especially) if the orchestrator process dies.

This allows a killed orchestrator process to be restarted as long as all of its remote
processes have gotten started working.

The remote process lease maintainers never _acquire_ the lease; they simply read what's in
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

from ._funcs import make_lease_uri
from .read import get_writer_id, make_read_leasefile
from .types import LeaseAcquired
from .write import LeaseEmitter, LeasefileWriter

MAINTAIN_LEASES = config.item("thds.mops.pure.local.maintain_leases", default=True, parse=config.tobool)
_MAINTENANCE_MARGIN = 0.5  # multiplier for the expire time
assert _MAINTENANCE_MARGIN < 1, "Maintenance margin must be less than 1 or leases will expire!"

_MAX_LEASES_PER_THREAD = 200  # I want to leave lots of margin so that leases don't expire.

logger = log.getLogger(__name__)


class _LeaseMaintenanceKit(ty.NamedTuple):
    wakeup_time: float
    lease_acquired: LeaseAcquired
    should_exit: ty.Callable[[], bool]


class _LeaseMaintenanceThreadState(ty.NamedTuple):
    heap: list[_LeaseMaintenanceKit]
    heap_lock: threading.Lock
    lease_added_event: threading.Event


@scope.bound
def _maintenance_daemon(state: _LeaseMaintenanceThreadState, daemon_num: int) -> None:
    """Daemon thread that maintains a set of leases."""
    scope.enter(log.logger_context(pid=os.getpid(), maint_daemon_num=daemon_num))
    log_at_level = logger.warning if daemon_num > 0 else logger.debug
    log_at_level("Starting lease maintenance daemon thread %s", daemon_num)

    while True:
        with state.heap_lock:
            if not state.heap:
                next_wakeup_time = None
            else:
                next_wakeup_time = state.heap[0].wakeup_time

        if next_wakeup_time is None:
            logger.debug("No leases to maintain; waiting indefinitely for new ones")
            state.lease_added_event.wait()
            state.lease_added_event.clear()
            continue

        # Wait until either: next maintenance time OR new lease added
        sleep_duration = max(0, next_wakeup_time - time.monotonic())
        woke_early = state.lease_added_event.wait(timeout=sleep_duration)
        state.lease_added_event.clear()

        if woke_early:
            continue  # go back to the beginning and check for the highest priority lease

        # Time to do maintenance
        while state.heap and state.heap[0].wakeup_time <= time.monotonic():
            with state.heap_lock:
                _, lease_obj, should_exit_fn = heapq.heappop(state.heap)

            if not should_exit_fn():
                try:
                    logger.debug("Maintaining lease %s", lease_obj.writer_id)
                    lease_obj.maintain()
                    # Re-schedule for next maintenance
                    with state.heap_lock:
                        next_maintenance = time.monotonic() + (lease_obj.expire_s * _MAINTENANCE_MARGIN)
                        heapq.heappush(
                            state.heap,
                            _LeaseMaintenanceKit(next_maintenance, lease_obj, should_exit_fn),
                        )
                except Exception:
                    logger.exception(f"Failed to maintain lease: {lease_obj}")


@dataclass
class _ShouldExit:
    lease_acquired: LeaseAcquired
    should_exit: bool = False

    def check_status(self) -> bool:
        return self.should_exit

    def stop_maintaining(self) -> None:
        self.should_exit = True
        self.lease_acquired.release()


_LEASE_RELEASERS_BY_ID = dict[str, ty.Callable[[], None]]()
_LEASE_MAINTENANCE_DAEMON_STATES = dict[int, _LeaseMaintenanceThreadState]()


@cache.locking
def _ensure_daemon(thread_num: int) -> None:
    """Start the maintenance daemon exactly once."""
    lease_state = _LeaseMaintenanceThreadState(
        heap=[],
        heap_lock=threading.Lock(),
        lease_added_event=threading.Event(),
    )
    assert thread_num not in _LEASE_MAINTENANCE_DAEMON_STATES  # protected by the cache.locking decorator
    _LEASE_MAINTENANCE_DAEMON_STATES[thread_num] = lease_state
    threading.Thread(target=_maintenance_daemon, args=(lease_state, thread_num), daemon=True).start()


def add_lease_to_maintenance_daemon(lease_acq: LeaseAcquired) -> ty.Callable[[], None]:
    """Add lease to global maintenance system and return a cleanup function."""
    if lease_acq.writer_id in _LEASE_RELEASERS_BY_ID:
        # technically we could be locking this, but mops itself does not allow
        # multiple callers to ask for the same lease to be maintained at the same time;
        # it will always be either the runner or the future that the runner has created.
        return _LEASE_RELEASERS_BY_ID[lease_acq.writer_id]

    should_exit = _ShouldExit(lease_acq)

    for i in range(len(_LEASE_MAINTENANCE_DAEMON_STATES) + 1):
        maintenance_daemon_state = _LEASE_MAINTENANCE_DAEMON_STATES.get(i)
        if maintenance_daemon_state is None:
            _ensure_daemon(i)
            maintenance_daemon_state = _LEASE_MAINTENANCE_DAEMON_STATES[i]
        elif len(maintenance_daemon_state.heap) > _MAX_LEASES_PER_THREAD:
            continue  # go to next thread if this one is too full

        with maintenance_daemon_state.heap_lock:
            next_time = time.monotonic() + (lease_acq.expire_s * _MAINTENANCE_MARGIN)
            heapq.heappush(
                maintenance_daemon_state.heap,
                _LeaseMaintenanceKit(next_time, lease_acq, should_exit.check_status),
            )
        maintenance_daemon_state.lease_added_event.set()
        break  # we found a thread that can take the lease

    _LEASE_RELEASERS_BY_ID[lease_acq.writer_id] = should_exit.stop_maintaining
    return should_exit.stop_maintaining


# from this point down, the code is about how to prepare to call add_lease_to_maintenance_daemon
# from the remote side, and what happens if the lease cannot or should not be maintained.


class CannotMaintainLease(ValueError):
    pass  # pragma: no cover


class LeaseLostError(ValueError):
    pass  # pragma: no cover


def make_remote_lease_writer(lease_dir_uri: str, expected_writer_id: str = "") -> LeaseAcquired:
    """Only for use by remote side - does not _acquire_ the lease,
    but merely allows for it to be maintained as unexpired. Does not allow for releasing,
    as it is not the responsibility of the remote side to release the lease.

    Will raise a CannotMaintainLease exception if the lease does not exist or has no
    expiration time.

    Will raise a LeaseLostError if a provided expected_writer_id (which is the
    writer_id of the lease as provided to the remote side by the original writer) does not
    match the lease's actual current writer_id - in other words, if some other writer has
    acquired the lease before the remote side has been able to start running.

    Notably, this is a race condition! The remote side depends on actual lease holders to
    cooperate in having only a single lease holder; the remote is simply checking a single
    time and then maintaining the lease indefinitely if the writer_id matches.

    TODO: If the lease is already expired but the writer_id still matches, perhaps we
    could be acquiring the lease to eliminate the race, and if we fail, we would
    exit with LeaseLostError...

    The return value is intended to be passed to add_lease_to_maintenance_daemon.
    """
    try:
        lease_uri = make_lease_uri(lease_dir_uri)
        read_leasefile = make_read_leasefile(lease_uri)
        lease_contents = read_leasefile()
    except Exception as exc:
        raise CannotMaintainLease(f"Could not read leasefile in lease dir: {lease_dir_uri}") from exc

    if not lease_contents:
        raise CannotMaintainLease(f"Lease does not exist: {lease_uri}")

    expire_s = lease_contents["expire_s"]
    if not expire_s or expire_s < 0:
        raise CannotMaintainLease(f"Lease is missing an expiry time: {lease_contents}")

    first_acquired_at_s = lease_contents["first_acquired_at"]
    if not first_acquired_at_s:
        raise CannotMaintainLease(f"Lease was never acquired: {lease_contents}")

    current_writer_id = lease_contents["writer_id"]
    if expected_writer_id and expected_writer_id != current_writer_id:
        raise LeaseLostError(
            "Refusing to maintain lease that was created by a different writer:"
            f" expected `{expected_writer_id}`, got `{current_writer_id}`."
            "This probably means you just need to kill and restart your orchestrator "
            " and it will begin awaiting the results of the new owner of the lease."
        )

    leasefile_writer = LeasefileWriter(
        current_writer_id,
        lease_dir_uri,
        LeaseEmitter(get_writer_id(lease_contents), timedelta(seconds=expire_s)),
        expire_s,
        writer_name="remote",
    )
    leasefile_writer.first_acquired_at = datetime.fromisoformat(first_acquired_at_s)
    # disable releasing from remote
    leasefile_writer.release = lambda: None  # type: ignore # noqa: E731
    return leasefile_writer


def maintain_to_release(acquired_lease: LeaseAcquired) -> ty.Callable[[], None]:
    """Depending on configuration, potentially start maintaining the lease.

    Return a callable that will release the lease when called.
    """
    if MAINTAIN_LEASES():
        return add_lease_to_maintenance_daemon(acquired_lease)

    return acquired_lease.release


def no_maintain() -> None:
    MAINTAIN_LEASES.set_global(False)
