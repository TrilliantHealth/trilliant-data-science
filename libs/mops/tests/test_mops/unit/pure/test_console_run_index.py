import datetime as dt
import logging

import pytest

from thds.mops import pure
from thds.mops.pure.tools.console import run_index

_STARTED = dt.datetime(2026, 8, 28, 18, 56, 51, tzinfo=dt.timezone.utc)


@pytest.fixture(autouse=True)
def _fresh():
    run_index._reset_for_test()
    yield
    run_index._reset_for_test()


def test_the_entry_name_reads_as_time_label_and_run():
    assert (
        run_index.entry_name(_STARTED, "nightly-2026-09", "2026-08-28/mr.SolidEat.ulewXQ")
        == "185651Z--nightly-2026-09--mr.SolidEat.ulewXQ"
    )


def test_the_time_is_utc_whatever_zone_the_start_was_recorded_in():
    eastern = _STARTED.astimezone(dt.timezone(dt.timedelta(hours=-4)))

    assert run_index.entry_name(eastern, "x", "mr.A").startswith("185651Z--")


def test_a_label_is_made_safe_for_a_blob_name():
    assert (
        run_index.entry_name(_STARTED, "weekly release / 2026 09", "mr.A")
        == "185651Z--weekly_release_2026_09--mr.A"
    )
    assert run_index.entry_name(_STARTED, "sam@laptop", "mr.A") == "185651Z--sam@laptop--mr.A"


def test_the_first_label_wins_and_a_disagreement_is_logged_once(caplog):
    pure.label_run("nightly-2026-09")
    with caplog.at_level(logging.WARNING):
        pure.label_run("nightly-2026-09")
        pure.label_run("inner-thing")
        pure.label_run("inner-thing")

    assert run_index.freeze_label("who@where") == "nightly-2026-09"
    assert [r.message for r in caplog.records] == [
        "The run is already labelled 'nightly-2026-09'; ignoring 'inner-thing'."
    ]
    # an outer wrapper names the run; what it calls into does not rename it, and agreeing
    # with the label already set is not worth a line.


def test_a_label_after_publication_takes_no_effect_and_says_so_once(caplog):
    assert run_index.freeze_label("sam@laptop") == "sam@laptop"

    with caplog.at_level(logging.WARNING):
        pure.label_run("too-late")
        pure.label_run("too-late")

    assert run_index.freeze_label("ignored") == "sam@laptop"
    assert len(caplog.records) == 1
    assert "published as 'sam@laptop'" in caplog.records[0].message
    assert "before the first mops call" in caplog.records[0].message


def test_publish_points_at_the_run_from_its_day(tmp_path):
    root = f"file://{tmp_path}/mops/console/2026-08-28/mr.SolidEat.ulewXQ"

    run_index.publish(root, _STARTED, "nightly-2026-09", "2026-08-28/mr.SolidEat.ulewXQ")

    pointer = tmp_path / "mops/console/2026-08-28/_index/185651Z--nightly-2026-09--mr.SolidEat.ulewXQ"
    assert pointer.read_text() == root + "\n"
