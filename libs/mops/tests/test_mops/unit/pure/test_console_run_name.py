"""Naming one orchestrator run, across every process that takes part in it."""

import datetime as dt
import os

from thds.mops.pure.tools.console import run_name

_A_DAY = dt.datetime(2026, 8, 9, 14, 30, tzinfo=dt.timezone.utc)


def test_the_day_is_the_directory():
    assert run_name.day_of(_A_DAY) == "2026-08-09"


def test_the_day_is_utc_whatever_the_local_clock_says():
    """Two machines in different zones must file the same run under the same day."""
    late = dt.datetime(2026, 8, 9, 23, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    assert run_name.day_of(late) == "2026-08-10"


def test_names_sort_chronologically_within_a_day():
    """The whole point of the temporal prefix: a listing is in the order runs started.

    Sorting only has to hold within a day because the day is in the path, so the temporal
    bits span a day rather than humenc's two-month default - which put every run started
    on one day into a single bucket, and left them unordered relative to each other.
    """
    midnight = _A_DAY.replace(hour=0, minute=0)
    through_the_day = [run_name.generate(midnight + dt.timedelta(hours=h)) for h in range(0, 24, 3)]

    assert through_the_day == sorted(through_the_day)


def test_two_runs_in_the_same_minute_do_not_collide():
    assert run_name.generate(_A_DAY) != run_name.generate(_A_DAY)


def test_a_name_carries_its_own_day():
    """So a process holding only the name can build the path the run started under."""
    assert run_name.current(True).startswith(f"{run_name.day_of(_now())}/mr.")


def test_the_name_is_minted_once_and_then_reused():
    assert run_name.current(True) == run_name.current(True)


def test_claiming_early_is_what_a_later_fork_inherits():
    """`claim` exists to be called before a process pool starts.

    A worker that mints its own name files its events somewhere nobody is reading. The
    environment only carries a name that already exists when the worker is created.
    """
    claimed = run_name.claim()

    assert claimed
    assert run_name.current(True) == claimed
    assert os.environ[run_name.RUN_NAME.envname] == claimed
    # the environment is the whole mechanism: a spawned worker reads its name from here
    # or mints one of its own.


def test_nothing_is_minted_when_the_console_is_off():
    """A name is what tells remotes to report, so an off console must not mint one."""
    assert run_name.current(False) == ""
    assert run_name.claim(False) == ""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)
