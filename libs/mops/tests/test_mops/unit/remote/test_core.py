import logging
import multiprocessing as mp

from thds.mops.remote import pure_remote


def a_function(a, b, c):
    return sum([a, b, c])


def test_pure_remote_bypass(caplog):
    caplog.set_level(logging.DEBUG)
    with mp.Pool() as pool:
        assert 6 == pure_remote(pool.apply, bypass_remote=lambda: True)(a_function)(1, 2, 3)

    assert len(caplog.records) == 0

    with mp.Pool() as pool:
        assert 6 == pure_remote(pool.apply, bypass_remote=True)(a_function)(1, 2, 3)

    assert len(caplog.records) == 0

    with mp.Pool() as pool:
        assert 6 == pure_remote(pool.apply)(a_function)(1, 2, 3)

    assert "Forwarding" in caplog.records[0].msg
