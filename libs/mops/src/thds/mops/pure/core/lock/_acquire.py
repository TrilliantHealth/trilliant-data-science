"""The intent of this module is to provide a best-effort "lock"
that can be built on top of just `getbytes` and `putbytes` operations.

It is important to note that this lock, while it should work under nearly all
circumstances, is not actually a true lock - it is _possible_ for multiple holders of the
lock to believe that they hold it exclusively, under degenerate conditions involving very
slow networks. Therefore, it should only be used as a performance optimization, and not in
cases where absolute application correctness depend upon an exclusive lock with full
guarantees.

While it is possible that a lock may be acquired multiple times, the _more likely_ failure
scenario is contention for the lock. There is a built in safety margin to reduce cases of
multiple lock acquirers, and with a very large number of lockers, it is possible that no
acquirer will ever get to see its own write 'persist' long enough to determine that it has
the lock.

Again, this algorithm is _not_ designed to be a perfect lock - only to make it relatively
efficient for a single caller to acquire a lock and maintain it for a period of time while
other potential acquirers instead determine that they ought to wait for the lock to be
released.
"""

import time
import timeit
import typing as ty
from datetime import datetime, timedelta
from uuid import uuid4

from thds import humenc
from thds.core import log

from . import _funcs
from .read import get_writer_id, make_read_lockfile
from .types import LockAcquired, LockContents
from .write import LockfileWriter, make_lock_contents

logger = log.getLogger(__name__)


def acquire(  # noqa: C901
    lock_dir_uri: str,
    *,
    expire: timedelta = timedelta(seconds=30),
    acquire_margin: timedelta = timedelta(seconds=0.0),
    debug: bool = True,
    block: ty.Optional[timedelta] = timedelta(seconds=0),
) -> ty.Optional[LockAcquired]:
    """Attempt to acquire an expiring lock.

    Return a callable suitable for 'maintaining the lock as active' if the lock was
    acquired, plus a Callable suitable for releasing the lock - otherwise, return None to
    indicate that the lock is not owned.

    The lock_dir_uri must be identical across multiple processes.

    It is strongly recommended that expire and acquire_margin also be identical
    across all processes attempting to acquire the same lock.

    It is up to the caller to call `lock.maintain` at regular intervals less than
    `expire`.  It is polite and more efficient for other acquirers for you to call
    `lock.release` when the lock is no longer needed.

    A lock that has not been updated in 'expire' seconds is considered 'released' and may
    be acquired by any other process attempting to acquire it. If you do not `.maintain()`
    the lock, you will lose it.

    `acquire_margin` is the minimum amount of time that will be waited after attempting to
    acquire the lock, to confirm that no other writer has also attempted to acquire
    it. This should be scaled to be longer than the longest delay you expect _any_
    candidate process to experience between checking the lock uri, finding it acquirable,
    and successfully writing back to the lock uri itself. If the default value (0) is
    provided, then the acquire_margin will be determined automatically to be twice the
    amount of time elapsed between the beginning of the check and the end of the write. If
    you have acquirers accessing this from very different environments, it may be safer to
    specify a higher acquire_margin that will be closer to the largest latency you expect
    any of your clients to experience.

    `block` is the _minimum_ amount of time to wait before returning None if the lock
    cannot be required. A zero length block will cause acquire to return None after the
    first unsuccessful attempt. Passing block=None will block until first acquisition.

    If you fail to acquire the lock and want to try again, it is recommended that you call
    this at spaced intervals, not in a tight loop, in order to avoid performance issues.

    """
    if acquire_margin * 2 > expire:
        # You should not be waiting nearly as much time as it would take for the lock to
        # become expire to decide that you have acquired the lock.
        #
        # If network or other delays are encountered, other candidate acquirers will end
        # up convinced that the lock has gone expire, right about the time you decide you
        # have acquired it.
        raise ValueError(
            f"Acquire margin ({acquire_margin.total_seconds()})"
            f" must be less than half the expire time ({expire.total_seconds()})."
        )

    acquire_margin_s = acquire_margin.total_seconds()
    if acquire_margin_s < 0:
        raise ValueError(f"Acquire margin may not be negative: {acquire_margin_s}")

    start = _funcs.utc_now()

    my_writer_id = humenc.encode(uuid4().bytes)

    lockfile_writer = LockfileWriter(
        my_writer_id,
        lock_dir_uri,
        make_lock_contents(my_writer_id, expire),
        expire.total_seconds(),
        debug=debug,
    )

    read_lockfile = make_read_lockfile(_funcs.make_lock_uri(lock_dir_uri))

    def is_released(lock_contents: LockContents) -> bool:
        return bool(lock_contents.get("released_at"))

    def is_fresh(lock_contents: LockContents) -> bool:
        written_at_str = lock_contents.get("written_at")
        if not written_at_str:
            # this likely won't happen in practice b/c we check released first.
            return False  # pragma: no cover
        lock_expire_s = lock_contents["expire_s"]
        if round(lock_expire_s, 4) != round(expire.total_seconds(), 4):
            logger.warning(
                f"Remote lock {lock_dir_uri} has expire duration {lock_expire_s},"
                f" which is different than the local configuration {expire}."
                " This may lead to multiple simultaneous acquirers on the lock."
            )
        return datetime.fromisoformat(written_at_str) + expire >= _funcs.utc_now()

    acquire_delay = 0.0

    def determine_acquire_delay(before_read: float) -> float:
        # decide how long we're going to wait.
        read_write_delay = timeit.default_timer() - before_read
        if acquire_margin_s and read_write_delay > acquire_margin_s:
            logger.warning(
                f"It took longer ({read_write_delay}) than the acquire margin"
                " between the lock check and completing the lock write."
                " There is danger that another process may think it has acquired the lock."
                " You should make the acquire_margin longer to reduce the chances of this happening."
            )
        auto_acquire_delay = read_write_delay * 2
        # pick the larger of the two, because if we're encountering bad latency, we should
        # be waiting longer to make sure that we don't 'think we won' because of latency.
        return max(acquire_margin_s, auto_acquire_delay)

    while True:
        before_read = timeit.default_timer()
        maybe_lock_contents = read_lockfile()
        if maybe_lock_contents:
            lock = maybe_lock_contents
            if is_released(lock):
                logger.debug("Lock %s was released - attempting to lock", lock_dir_uri)
            elif not is_fresh(lock):
                logger.debug("Lock %s has expired - will attempt to steal it!", lock_dir_uri)
            elif get_writer_id(lock) == my_writer_id:
                # LOCK ACQUIRED!
                lockfile_writer.mark_acquired()
                # You still need to maintain it by calling .maintain() periodically!
                return lockfile_writer

            else:
                # lock is fresh and held by another acquirer - failed to acquire!
                if acquire_delay:
                    logger.info(f"Lost race for lock {lock_dir_uri}")
                    # this is info (not debug) because we expect it to be rare.
                    acquire_delay = 0.0
                if block is not None and _funcs.utc_now() > start + block:
                    return None

                time.sleep(0.2)
                # just a short sleep before we try again - this probably doesn't need to
                # be configurable, since anyone wanting different behavior can just pass
                # block=0.0 and then do the polling themselves.
                continue
        else:
            logger.debug("Lock %s does not exist - will attempt to lock it.", lock_dir_uri)

        # lock has expired or does not exist - attempt to acquire it by writing!
        lockfile_writer.write()

        # wait for a long enough time that we feel confident we were the last writer and
        # not just the fastest write-then-reader.
        acquire_delay = determine_acquire_delay(before_read)
        logger.debug(
            "Waiting %s seconds before checking lock to see if we acquired it...", acquire_delay
        )
        time.sleep(acquire_delay)
        # go back to the beginning of the loop, and see if we managed to acquire the lock!
