import re
import sqlite3
import time
from datetime import timedelta

import pytest

from thds.core import scope, tmp
from thds.core.progress import calc_report_every, report_gen, report_still_alive


@pytest.mark.parametrize(
    "target_interval, total, sec_elapsed, expected",
    [
        (20, 1, 100, 1),
        (20, 1, 20, 1),
        (20, 1, 10, 2),
        (20, 2, 10, 5),
        (20, 2, 5, 10),
        (20, 400, 10, 1000),
        (20, 800, 20, 1000),
        (20, 1600, 40, 1000),
        (20, 10000, 10, 20000),
        (20, 20000, 20, 20000),
        (20, 30000, 30, 20000),
        (20, 40000, 40, 20000),
    ],
)
def test_basic_even_numbers(target_interval, total, sec_elapsed, expected):
    assert calc_report_every(target_interval, total, sec_elapsed) == expected


@pytest.mark.parametrize(
    "target_interval, total, sec_elapsed, expected",
    [
        (20, 15_423, 10, 50_000),
        (20, 15_423, 20, 20_000),
        (20, 15_423, 30, 10_000),
        (20, 15_423, 40, 10_000),
        (20, 2_342_345, 10, 5_000_000),
        (20, 2_342_345, 20, 2_000_000),
        (20, 500_000, 10, 1_000_000),
        (20, 1_000_000, 20, 1_000_000),
        (20, 10_000_000, 200, 1_000_000),
    ],
)
def test_less_obvious_quantities(target_interval, total, sec_elapsed, expected):
    assert calc_report_every(target_interval, total, sec_elapsed) == expected


def test_report_gen():
    def gen():
        for i in range(100):
            yield i

    assert list(report_gen(gen)()) == list(range(100))


def test_report_still_alive(caplog):
    def _takes_a_long_time(n: int) -> int:
        time.sleep(0.5)
        return n + 1

    result = report_still_alive(roughly_every_s=timedelta(seconds=0.2))(_takes_a_long_time)(1)
    assert result == 2
    assert re.search(r"Still working after \d+\.\d+ seconds", caplog.text)


@scope.bound
def test_report_still_alive_can_handle_sqlite_connections():
    tmp_db = scope.enter(tmp.temppath_same_fs())

    def _populate_table(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            create table t (x integer, y integer);
            insert into t (x, y) values (1, 2);
            insert into t (x, y) values (3, 4);
            """
        )

    conn = sqlite3.connect(tmp_db)
    report_still_alive(roughly_every_s=timedelta(seconds=0.1))(_populate_table)(conn)
