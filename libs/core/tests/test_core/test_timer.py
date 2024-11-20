import logging
import time
import unittest.mock

import pytest

from thds.core.timer import TimeTracker, timer


def test_timer(caplog):
    caplog.set_level(logging.INFO)

    @timer
    def foo():
        x = 1 + 1
        return x

    foo()
    for record in caplog.records:
        assert record.levelname == "INFO"
    assert "Starting 'foo'" in caplog.records[0].message
    assert "Finished 'foo' in" in caplog.records[1].message


class MockTime:
    def __init__(self):
        self.counter = 0.0

    def sleep(self, t):
        self.counter += t

    def perf_counter(self):
        return self.counter


@pytest.fixture(scope="function")
def mock_time():
    mock = MockTime()
    with unittest.mock.patch("time.sleep", mock.sleep), unittest.mock.patch(
        "time.perf_counter", mock.perf_counter
    ):
        yield


def test_timer_decorator(mock_time):
    tracker = TimeTracker()

    @tracker.track("wait")
    def wait(secs: int):
        time.sleep(secs)

    wait(2)

    assert tracker.tracked_times["wait"].secs == 2.0
    assert tracker.tracked_times["wait"].calls == 1

    wait(1)

    assert tracker.tracked_times["wait"].secs == 3.0
    assert tracker.tracked_times["wait"].calls == 2


def test_timer_decorator_with_total_context(mock_time):
    tracker = TimeTracker()

    @tracker.track("wait")
    def wait(secs: int):
        time.sleep(secs)

    with tracker(tracker.total):
        wait(2)
        wait(1)

    assert tracker.tracked_times[tracker.total].secs == 3.0
    assert tracker.tracked_times["wait"].secs == 3.0
    assert tracker.tracked_times["wait"].pct_of_total(tracker.tracked_times[tracker.total].secs) == 100.0


def test_nested_context_tracking(mock_time):
    tracker = TimeTracker()

    def wait(secs: int):
        time.sleep(secs)

    with tracker(tracker.total) as t1:
        wait(5)
        with t1("nested_wait_2"):
            wait(2)
        with t1("nested_wait_2_and_5") as t2:
            wait(2)
            with t2("nested_wait_5"):
                wait(5)

    assert tracker.tracked_times[tracker.total].secs == 14.0
    assert tracker.tracked_times["nested_wait_2"].secs == 2.0
    assert tracker.tracked_times["nested_wait_2_and_5"].secs == 7.0
    assert tracker.tracked_times["nested_wait_5"].secs == 5.0


@pytest.mark.parametrize("num_loops", [1, 2, 4])
def test_context_tracking_in_loop(num_loops: int, mock_time):
    tracker = TimeTracker()

    def wait(secs: int):
        time.sleep(secs)

    for _ in range(num_loops):
        with tracker(tracker.total) as t1:
            wait(5)
            with t1("nested_wait_2"):
                wait(2)
            with t1("nested_wait_2_and_5") as t2:
                wait(2)
                with t2("nested_wait_5"):
                    wait(5)

    assert tracker.tracked_times[tracker.total].secs == 14.0 * num_loops
    assert tracker.tracked_times["nested_wait_2"].secs == 2.0 * num_loops
    assert tracker.tracked_times["nested_wait_2_and_5"].secs == 7.0 * num_loops
    assert tracker.tracked_times["nested_wait_5"].secs == 5.0 * num_loops

    total = tracker.tracked_times[tracker.total].secs
    assert tracker.tracked_times[tracker.total].pct_of_total(total) == 100.0
    assert tracker.tracked_times["nested_wait_2"].pct_of_total(total) == (2.0 / 14.0) * 100.0
