# very basic lease validation.
import time
import typing as ty
from datetime import timedelta
from threading import Thread
from timeit import default_timer

import pytest

from thds.core import tmp
from thds.mops.pure.core.lease import acquire
from thds.mops.pure.core.lease.maintain import LeaseLostError, make_remote_lease_writer
from thds.mops.pure.core.lease.read import make_read_leasefile

SHORT = timedelta(seconds=0.3)


def acquire_the_lease(uri: str, accum: list):
    leased = acquire(uri, block=timedelta(seconds=3), acquire_margin=SHORT)
    if leased:
        start = default_timer()
        while default_timer() - start < 6:
            time.sleep(0.1)
            leased.maintain()
        leased.release()
        accum.append(1)
    else:
        accum.append(0)


@pytest.fixture
def lease_uri() -> ty.Iterator[str]:
    """We can test these against the local filesystem to make things faster"""
    with tmp.tempdir_same_fs() as leasedir:
        leasedir.mkdir(exist_ok=True, parents=True)
        yield f"file://{leasedir}"


def test_many_acquirers_but_only_one_gets_it(lease_uri):
    accum = list()  # type: ignore
    threads = [Thread(target=acquire_the_lease, args=(lease_uri, accum)) for _ in range(4)]
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


def test_lease_can_be_acquired_after_released(lease_uri):
    leased = acquire(lease_uri, acquire_margin=SHORT)
    assert leased
    assert not acquire(lease_uri, acquire_margin=SHORT)
    leased.release()

    leased = acquire(lease_uri, acquire_margin=SHORT)
    assert leased


def test_lease_blocking_works(lease_uri):
    leased = acquire(lease_uri, acquire_margin=SHORT)
    assert leased
    assert not acquire(lease_uri, block=timedelta(seconds=3), acquire_margin=SHORT)
    leased.release()
    leased = acquire(lease_uri, acquire_margin=SHORT)


def test_not_fresh_lease(lease_uri):
    leased = acquire(lease_uri, acquire_margin=SHORT, expire=SHORT * 2)
    assert leased
    time.sleep(SHORT.total_seconds() * 2)
    # lease will now have expired
    assert acquire(lease_uri, acquire_margin=SHORT, expire=SHORT * 2)


def test_maintain(lease_uri):
    assert acquire(lease_uri, acquire_margin=SHORT, expire=timedelta(seconds=4))

    maintainer = make_remote_lease_writer(lease_uri)
    assert maintainer.expire_s == 4.0

    maintainer.maintain()  # just needs to not error

    lease_contents = make_read_leasefile(lease_uri + "/lock.json")()
    assert lease_contents
    assert lease_contents["expire_s"] == maintainer.expire_s
    assert lease_contents["first_acquired_at"]  # must always be acquired
    assert lease_contents["write_count"] == 1
    assert not lease_contents["released_at"]


def test_beaten_remote_maintainer_gives_up_early(lease_uri):
    leased = acquire(lease_uri, acquire_margin=SHORT, expire=timedelta(seconds=1))
    assert leased
    beaten_writer_id = leased.writer_id

    time.sleep(2)  # lease has expired
    leased_2 = acquire(lease_uri, acquire_margin=SHORT, expire=timedelta(seconds=1))
    assert leased_2
    assert beaten_writer_id != leased_2.writer_id

    with pytest.raises(LeaseLostError):
        make_remote_lease_writer(lease_uri, expected_writer_id=beaten_writer_id)

    make_remote_lease_writer(lease_uri, expected_writer_id=leased_2.writer_id)
    make_remote_lease_writer(lease_uri, expected_writer_id="")  # nothing expected so it's fine
