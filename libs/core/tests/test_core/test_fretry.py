import pytest

from thds.core.fretry import (
    expo,
    is_exc,
    iter_to_async,
    n_times,
    retry_regular,
    retry_regular_async,
    retry_sleep,
    retry_sleep_async,
    sleep,
)


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
    assert no_jitter_run == (0.0, 1.0, 3.0, 7.0)

    jitter_run_1 = tuple(expo(retries=4)())
    assert jitter_run_1 != no_jitter_run

    def check_jitter_run(run):
        assert run[0] == 0.0, run
        # the total amount of time should be roughly the same, because
        # we don't allow jitter to compound itself over time.
        assert sum(run) >= sum(no_jitter_run[:3]) + no_jitter_run[3] * 0.5
        assert sum(run) <= sum(no_jitter_run[:3]) + no_jitter_run[3] * 1.5

    check_jitter_run(jitter_run_1)
    # check one manually, for debuggability

    jitter_runs = [jitter_run_1]

    for _ in range(10):
        jitter_run = tuple(expo(retries=4)())
        check_jitter_run(jitter_run)
        jitter_runs.append(jitter_run)

    assert len(set(jitter_runs)) == len(jitter_runs), jitter_runs  # they're all different


@pytest.mark.asyncio
async def test_retry_regular_async():
    count = 0

    @retry_regular_async(lambda e: True, iter_to_async(n_times(4)))
    async def broken():
        nonlocal count
        count += 1
        print(count)
        raise ValueError(str(count))

    with pytest.raises(ValueError):
        await broken()

    assert count == 5
    with pytest.raises(ValueError):
        await broken()

    assert count == 10  # a second attempt goes through the retry loop again.


@pytest.mark.asyncio
async def test_retry_sleep_async():
    count = 0

    @retry_sleep_async(is_exc(ValueError), expo(retries=4, delay=0.0))
    async def broken():
        nonlocal count
        count += 1
        print(count)
        raise ValueError(str(count))

    with pytest.raises(ValueError):
        await broken()

    assert count == 5
