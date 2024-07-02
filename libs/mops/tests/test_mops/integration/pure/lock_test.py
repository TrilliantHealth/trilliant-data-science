# very basic lock validation.
import time
from datetime import datetime, timedelta
from threading import Thread
from timeit import default_timer

import pytest

from thds.mops.pure.core.lock import acquire

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
        print("got the lock")
    else:
        accum.append(0)
        print("did not get the lock")


@pytest.fixture
def lock_uri() -> str:
    return f"adls://thdsscratch/tmp/test/mops.pure.core.lock/lock-{datetime.now().isoformat()}"


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
