import logging
import multiprocessing as mp

from thds.mops.pure import use_runner


def a_function(a, b, c):
    return sum([a, b, c])


def test_use_runner_skip_runner(caplog):
    caplog.set_level(logging.DEBUG)
    with mp.Pool() as pool:
        assert 6 == use_runner(pool.apply, skip=lambda: True)(a_function)(1, 2, 3)

    assert "Forwarding" not in caplog.records[0].msg


def test_use_runner_dont_skip_runner(caplog):
    caplog.set_level(logging.DEBUG)
    with mp.Pool() as pool:
        assert 6 == use_runner(pool.apply)(a_function)(1, 2, 3)

    assert "Forwarding" in caplog.records[0].msg
