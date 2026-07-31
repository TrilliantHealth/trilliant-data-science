import os
import sys
import threading
import typing as ty
from dataclasses import dataclass
from datetime import datetime, timedelta

from thds.core import hostname, log, thread_debug

from . import _funcs
from .types import LeaseContents

logger = log.getLogger(__name__)


@dataclass
class LeaseEmitter:
    writer_id: str
    expire: timedelta

    write_count: int = 0
    first_written_at: str = ""

    def __post_init__(self) -> None:
        assert "/" not in self.writer_id, (
            f"{self.writer_id} should not contain a slash - maybe you passed a URI instead?"
        )

    def __call__(self, first_acquired_at: ty.Optional[datetime]) -> LeaseContents:
        self.write_count += 1
        now = _funcs.utc_now().isoformat()
        self.first_written_at = self.first_written_at or now

        return {
            "writer_id": self.writer_id,
            "written_at": now,
            "expire_s": self.expire.total_seconds(),
            # debug stuff:
            "write_count": self.write_count,
            "hostname": hostname.friendly(),
            "pid": str(os.getpid()),
            "tid": threading.get_ident(),
            "first_written_at": self.first_written_at,
            "first_acquired_at": first_acquired_at.isoformat() if first_acquired_at else "",
            "released_at": "",
        }


def _pid_tid() -> str:
    return f"{os.getpid()}-{threading.get_ident()}"


def _capture_thread_info() -> dict[str, ty.Any]:
    return dict(
        ppid=os.getppid(),
        process_cmd=[" ".join(sys.argv)],
        **thread_debug.capture_thread_context(),
    )


class _Debug:
    def __init__(self) -> None:
        self.original_pid = os.getpid()
        self.original_thread_id = threading.get_ident()
        self.thread_info: dict[str, dict] = {_pid_tid(): _capture_thread_info()}


class LeasefileWriter:
    """The core purpose of this class is to allow setting of first_acquired_at immediately
    after the first time that it is confirmed that we have acquired the lease.

    Everything else could have been done as a (simpler) closure.
    """

    def __init__(
        self,
        lease_writer_id: str,
        lease_dir_uri: str,
        generate_lease: ty.Callable[[ty.Optional[datetime]], LeaseContents],
        expire_s: float,
        *,
        debug: bool = True,
        writer_name: str = "",
    ) -> None:
        self.writer_id = lease_writer_id
        self.lease_dir_uri = lease_dir_uri
        self.blob_store, self.lease_uri = _funcs.store_and_lease_uri(lease_dir_uri)
        self.generate_lease = generate_lease
        self.expire_s = expire_s
        self.debug = _Debug() if debug else None
        self.writer_name = writer_name
        self.first_acquired_at: ty.Optional[datetime] = None

    def mark_acquired(self) -> None:
        assert not self.first_acquired_at
        self.first_acquired_at = _funcs.utc_now()
        logger.debug("Acquired lease %s", self.lease_uri)
        self.write()  # record the first_acquired_at value for posterity

    def write(self) -> None:
        lease_contents = self.generate_lease(self.first_acquired_at)
        if self.writer_name:
            lease_contents["writer_name"] = self.writer_name  # type: ignore
        assert "/" not in lease_contents["writer_id"], lease_contents
        assert self.writer_id == lease_contents["writer_id"], (self.writer_id, lease_contents)
        lease_bytes = _funcs.json_dumpb(lease_contents)
        assert lease_bytes
        # technically, writing these bytes may cause an overwrite of someone else's lease.
        # the only way we get to 'decide' who acquired the lease is by waiting an
        # appropriate period of time (agreed upon by all acquirers, and sufficient to be
        # certain that everyone who tried is going to actually wait long enough to see the
        # results - and then we see who wrote it last. Whoever wrote it last 'won',
        # and should continue as though they acquired the lease. Everyone else should 'fail'
        # to acquire the lease.
        _funcs.write(self.blob_store, self.lease_uri, lease_bytes)
        self._maybe_write_debug(lease_contents)

    def maintain(self) -> None:
        """It is valid to call this method multiple times as necessary once the lease has been acquired."""
        self.write()

    def release(self) -> None:
        assert self.first_acquired_at
        lease_contents = self.generate_lease(self.first_acquired_at)
        lease_contents["released_at"] = lease_contents["written_at"]
        lease_contents["written_at"] = ""
        logger.debug(
            "Releasing lease %s after %s", self.lease_uri, _funcs.utc_now() - self.first_acquired_at
        )
        _funcs.write(self.blob_store, self.lease_uri, _funcs.json_dumpb(lease_contents))
        self._maybe_write_debug(lease_contents)

    def _maybe_write_debug(self, lease_contents: LeaseContents) -> None:
        """Only do this if the lease was actually acquired."""
        # this debug bit serves to help us understand when clients actually believed
        # that they had acquired the lease.  Because we only do this after our first
        # 'successful' write, it will not impose extra latency during the
        # latency-critical section.
        if self.debug and self.first_acquired_at:
            pid = lease_contents["pid"]
            tid = lease_contents["tid"]
            pid_tid = f"{pid}-{tid}"
            if pid_tid not in self.debug.thread_info:
                self.debug.thread_info[pid_tid] = _capture_thread_info()

            name = (";_name=" + self.writer_name) if self.writer_name else ""
            first_written_at = lease_contents["first_written_at"]
            hostname = lease_contents["hostname"]

            acq_uuid = lease_contents["writer_id"]
            assert "/" not in acq_uuid, lease_contents
            debug_uri = self.blob_store.join(
                self.lease_dir_uri,
                "writers-debug",
                f"firstwrite={first_written_at};_uuid={acq_uuid};_host={hostname};_pid={pid};_tid={tid}{name}.json",
            )
            try:
                self.blob_store.putbytes(
                    debug_uri,
                    _funcs.json_dumpb(
                        dict(
                            lease_contents,
                            thread_debug=dict(
                                original_pid=self.debug.original_pid,
                                original_thread_id=self.debug.original_thread_id,
                                thread_info=self.debug.thread_info,
                            ),
                        )
                    ),
                    type_hint="application/mops-lease-breadcrumb",
                )
            except Exception:
                logger.warning(f"Problem writing debug lease {debug_uri}")
