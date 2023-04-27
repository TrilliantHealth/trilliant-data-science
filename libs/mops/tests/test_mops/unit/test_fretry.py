import pytest

from thds.mops.fretry import expo, retry_regular, sleep


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
