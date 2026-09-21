"""A stopped lease registration must not be handed back to the next registrant of the
same writer id - a long-lived process that re-runs an invocation needs its new lease
actually maintained."""

import time
import typing as ty

from thds.mops.pure.core.lease import maintain


class _CountingLease:
    writer_id = "reused-writer"
    expire_s = 0.05

    def __init__(self) -> None:
        self.maintained = 0

    def maintain(self) -> None:
        self.maintained += 1

    def release(self) -> None:
        pass


def _wait_for(pred: ty.Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not pred():
        assert time.monotonic() < deadline, "condition never held"
        time.sleep(0.01)


def test_registration_is_reusable_after_stop():
    first = _CountingLease()
    stop_first = maintain.add_lease_to_maintenance_daemon(first)
    _wait_for(lambda: first.maintained >= 1)
    stop_first()

    second = _CountingLease()  # same writer id, a new lease object
    stop_second = maintain.add_lease_to_maintenance_daemon(second)
    assert stop_second is not stop_first
    _wait_for(lambda: second.maintained >= 1)
    stop_second()
