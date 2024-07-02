import os
import typing as ty
from datetime import datetime, timedelta

from thds.core import hostname, log

from . import _funcs
from .types import LockContents

logger = log.getLogger(__name__)


def make_lock_contents(
    lock_uuid: str, expire: timedelta
) -> ty.Callable[[ty.Optional[datetime]], LockContents]:
    """Impure - Resets written_at to 'right now' to keep the lock 'live'."""
    write_count = 0

    def lock_contents(first_acquired_at: ty.Optional[datetime]) -> LockContents:
        nonlocal write_count
        write_count += 1
        return {
            "lock_uuid": lock_uuid,
            "hostname": hostname.friendly(),
            "pid": str(os.getpid()),
            "written_at": _funcs.utc_now().isoformat(),
            "expire_s": expire.total_seconds(),
            "write_count": write_count,
            "first_acquired_at": first_acquired_at.isoformat() if first_acquired_at else "",
            "released_at": "",
        }

    return lock_contents


class LockfileWriter:
    """The core purpose of this class is to allow setting of first_acquired_at immediately
    after the first time that it is confirmed that we have acquired the lock.

    Everything else could have been done as a (simpler) closure.
    """

    def __init__(
        self,
        lock_dir_uri: str,
        generate_lock: ty.Callable[[ty.Optional[datetime]], LockContents],
        expire_s: float,
        *,
        debug: bool = True,
    ):
        self.lock_dir_uri = lock_dir_uri
        self.blob_store, self.lock_uri = _funcs.store_and_lock_uri(lock_dir_uri)
        self.generate_lock = generate_lock
        self.expire_s = expire_s
        self.debug = debug
        self.first_acquired_at: ty.Optional[datetime] = None

    def mark_acquired(self):
        assert not self.first_acquired_at
        self.first_acquired_at = _funcs.utc_now()
        logger.debug("Acquired lock %s", self.lock_uri)
        self.write()  # record the first_acquired_at value for posterity

    def write(self) -> None:
        lock_contents = self.generate_lock(self.first_acquired_at)
        lock_bytes = _funcs.json_dumpb(lock_contents)
        assert lock_bytes
        # technically, writing these bytes may cause an overwrite of someone else's lock.
        # the only way we get to 'decide' who acquired the lock is by waiting an
        # appropriate period of time (agreed upon by all acquirers, and sufficient to be
        # certain that everyone who tried is going to actually wait long enough to see the
        # results - and then we see who wrote it last. Whoever wrote it last 'won',
        # and should continue as though they acquired the lock. Everyone else should 'fail'
        # to acquire the lock.
        _funcs.write(self.blob_store, self.lock_uri, lock_bytes)
        self._write_debug(lock_contents)

    def maintain(self) -> None:
        """It is valid to call this method multiple times as necessary once the lock has been acquired."""
        self.write()

    def release(self) -> None:
        assert self.first_acquired_at
        lock_contents = self.generate_lock(self.first_acquired_at)
        lock_contents["released_at"] = lock_contents["written_at"]
        lock_contents["written_at"] = ""
        logger.debug(
            "Releasing lock %s after %s", self.lock_uri, _funcs.utc_now() - self.first_acquired_at
        )
        _funcs.write(self.blob_store, self.lock_uri, _funcs.json_dumpb(lock_contents))
        self._write_debug(lock_contents)

    def _write_debug(self, lock_contents: LockContents) -> None:
        # this debug bit serves to help us understand when clients actually believed
        # that they had acquired the lock.  Because we only do this after our first
        # 'successful' write, it will not impose extra latency during the
        # latency-critical section.
        if self.debug and self.first_acquired_at:
            hostname = lock_contents["hostname"]
            pid = lock_contents["pid"]
            lock_uuid = lock_contents["lock_uuid"]
            debug_uri = self.blob_store.join(
                self.lock_dir_uri,
                "acquirers-debug",
                f"{hostname}-{pid}-{lock_uuid}-lock.json",
            )
            try:
                self.blob_store.putbytes(
                    debug_uri,
                    _funcs.json_dumpb(lock_contents),
                    type_hint="lock-breadcrumb",
                )
            except Exception:
                logger.warning(f"Problem writing debug lock {debug_uri}")
