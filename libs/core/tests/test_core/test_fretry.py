import pytest

from thds.core.fretry import expo, is_exc, retry_regular, retry_sleep, sleep


def test_retry_regular():
    count = 0

    @retry_regular(lambda e: True, sleep(expo(retries=4, delay=0.0)))
    def broken():
        nonlocal count
        count += 1
        print(count)
        raise ValueError(str(count))

    with pytest.raises(ValueError):
        broken()

    assert count == 5
    with pytest.raises(ValueError):
        broken()

    assert count == 10  # a second attempt goes through the retry loop again.


def test_retry_sleep():
    count = 0

    @retry_sleep(is_exc(ValueError), expo(retries=4, delay=0.0))
    def broken():
        nonlocal count
        count += 1
        print(count)
        raise ValueError(str(count))

    with pytest.raises(ValueError):
        broken()

    assert count == 5


def test_expo_jitter():
    no_jitter_run = tuple(expo(retries=4, jitter=False)())
    assert no_jitter_run == (1.0, 2.0, 4.0, 8.0)

    jitter_run_1 = tuple(expo(retries=4)())
    assert jitter_run_1 != no_jitter_run

    def check_jitter_run(run):
        assert 0.5 <= run[0] <= 1.5, run
        assert 1.0 <= run[1] <= 3.0, run
        assert 2.0 <= run[2] <= 6.0, run
        assert 4.0 <= run[3] <= 12.0, run

    check_jitter_run(jitter_run_1)
    # check one manually, for debuggability

    jitter_runs = [jitter_run_1]

    for _ in range(10):
        jitter_run = tuple(expo(retries=4)())
        check_jitter_run(jitter_run)
        jitter_runs.append(jitter_run)

    assert len(set(jitter_runs)) == len(jitter_runs), jitter_runs  # they're all different
