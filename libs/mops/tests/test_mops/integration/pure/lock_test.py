# very basic lock validation.
import time
import typing as ty
from datetime import timedelta
from threading import Thread
from timeit import default_timer

import pytest

from thds.core import tmp
from thds.mops.pure.core.lock import acquire
from thds.mops.pure.core.lock.maintain import LockWasStolenError, remote_lock_maintain
from thds.mops.pure.core.lock.read import make_read_lockfile

SHORT = timedelta(seconds=0.3)


def acquire_the_lock(uri: str, accum: list):
    locked = acquire(uri, block=timedelta(seconds=3), acquire_margin=SHORT)
    if locked:
        start = default_timer()
        while default_timer() - start < 6:
            time.sleep(0.1)
            locked.maintain()
        locked.release()
        accum.append(1)
    else:
        accum.append(0)


@pytest.fixture
def lock_uri() -> ty.Iterator[str]:
    """We can test these against the local filesystem to make things faster"""
    with tmp.tempdir_same_fs() as lockdir:
        lockdir.mkdir(exist_ok=True, parents=True)
        yield f"file://{lockdir}"


def test_many_acquirers_but_only_one_gets_it(lock_uri):
    accum = list()  # type: ignore
    threads = [Thread(target=acquire_the_lock, args=(lock_uri, accum)) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(accum) == 1


def test_disallow_bad_acquire_margin():
    with pytest.raises(ValueError):
        acquire("foobar", acquire_margin=timedelta(seconds=20), expire=timedelta(seconds=21))

    with pytest.raises(ValueError):
        acquire("foobar", acquire_margin=timedelta(seconds=-2))


def test_lock_can_be_acquired_after_released(lock_uri):
    locked = acquire(lock_uri, acquire_margin=SHORT)
    assert locked
    assert not acquire(lock_uri, acquire_margin=SHORT)
    locked.release()

    locked = acquire(lock_uri, acquire_margin=SHORT)
    assert locked


def test_lock_blocking_works(lock_uri):
    locked = acquire(lock_uri, acquire_margin=SHORT)
    assert locked
    assert not acquire(lock_uri, block=timedelta(seconds=3), acquire_margin=SHORT)
    locked.release()
    locked = acquire(lock_uri, acquire_margin=SHORT)


def test_not_fresh_lock(lock_uri):
    locked = acquire(lock_uri, acquire_margin=SHORT, expire=SHORT * 2)
    assert locked
    time.sleep(SHORT.total_seconds() * 2)
    # lock will now have expired
    assert acquire(lock_uri, acquire_margin=SHORT, expire=SHORT * 2)


def test_maintain(lock_uri):
    assert acquire(lock_uri, acquire_margin=SHORT, expire=timedelta(seconds=4))

    maintainer = remote_lock_maintain(lock_uri)
    assert maintainer.expire_s == 4.0

    maintainer.maintain()  # just needs to not error

    lock_contents = make_read_lockfile(lock_uri + "/lock.json")()
    assert lock_contents
    assert lock_contents["expire_s"] == maintainer.expire_s
    assert lock_contents["first_acquired_at"]  # must always be acquired
    assert lock_contents["write_count"] == 1
    assert not lock_contents["released_at"]


def test_beaten_remote_maintainer_gives_up_early(lock_uri):
    locked = acquire(lock_uri, acquire_margin=SHORT, expire=timedelta(seconds=1))
    assert locked
    beaten_writer_id = locked.writer_id

    time.sleep(2)  # lock has expired
    locked_2 = acquire(lock_uri, acquire_margin=SHORT, expire=timedelta(seconds=1))
    assert locked_2
    assert beaten_writer_id != locked_2.writer_id

    with pytest.raises(LockWasStolenError):
        remote_lock_maintain(lock_uri, expected_writer_id=beaten_writer_id)

    remote_lock_maintain(lock_uri, expected_writer_id=locked_2.writer_id)
    remote_lock_maintain(lock_uri, expected_writer_id="")  # nothing expected so it's fine
