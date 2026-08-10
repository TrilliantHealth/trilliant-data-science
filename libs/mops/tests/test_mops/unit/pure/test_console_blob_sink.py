import datetime as dt
import json

import pytest

from thds.mops.pure.tools import console
from thds.mops.pure.tools.console import blob_sink, runtime, throwaway
from thds.mops.pure.tools.console.events import Event

_AT = dt.datetime(2026, 8, 6, 12, 0, tzinfo=dt.timezone.utc)


@pytest.fixture
def real_run(monkeypatch):
    """Make this process look like someone's own run rather than a test.

    These tests run under pytest, which is one of the things that sends events to the
    throwaway location - so anything asserting where a real run's events go has to say so.

    The environment cannot be cleared to achieve this: pytest sets `PYTEST_CURRENT_TEST`
    for each test after its fixtures have run, so it is back by the time the test body
    calls anything.
    """
    monkeypatch.setattr(throwaway, "here", lambda: False)


def _memo_uri(root: str) -> str:
    return f"{root}/mops2-mpf/pipe/pkg.mod--fn/hash123"


def test_an_object_name_says_which_shape_it_holds():
    """A remote writes one event per object; an orchestrator writes a batch of them, one
    per line. Readers sniff the content, but whoever opens one by hand should not have to."""
    event = Event(event="started", at="2026-08-07T12:00:00+00:00")

    assert blob_sink.object_name(event, "attempt-started").endswith(".json")
    assert blob_sink.object_name(event, "orchestrator-1-0", lines=True).endswith(".jsonl")


def test_a_test_run_is_kept_out_of_the_way():
    """Automated suites produce runs continuously, and mixing them in with real ones makes
    the real ones hard to find."""
    root = "file:///tmp/r"

    assert "/mops/console-throwaway/" in blob_sink.events_root(_memo_uri(root), "mr.Run.abc")


def test_a_real_run_goes_where_people_look(real_run):
    root = "file:///tmp/r"

    assert blob_sink.events_root(_memo_uri(root), "mr.Run.abc") == f"{root}/mops/console/mr.Run.abc"


def test_throwaway_runs_are_still_written(real_run, monkeypatch):
    """Redirected, not suppressed: a suite that writes nowhere stops covering the write
    path, and a break in it would surface first in someone's real run."""
    monkeypatch.setenv("CI", "true")

    assert blob_sink.events_root(_memo_uri("file:///tmp/r"), "mr.Run.abc")


def test_events_root_follows_the_invocation_not_the_configured_root(tmp_path, real_run):
    """The blob store's configured root is not necessarily where this invocation lives -
    a run can span roots, and its events must follow the work."""
    root = f"file://{tmp_path}/somewhere-specific"

    assert blob_sink.events_root(_memo_uri(root), "mr.Run.abc") == (f"{root}/mops/console/mr.Run.abc")


def test_events_are_a_sibling_of_the_memoization_namespace(tmp_path):
    root = f"file://{tmp_path}/r"

    assert "mops2-mpf" not in blob_sink.events_root(_memo_uri(root), "mr.Run.abc")


def test_emit_writes_nothing_when_remote_events_are_off(tmp_path):
    """Remotes report by default; this is the switch that silences one."""
    uri = _memo_uri(f"file://{tmp_path}/r")
    with blob_sink.CONSOLE_REMOTE_EVENTS.set_local(False):
        blob_sink.emit_to_blob(uri, "mr.Run.abc", console.started(uri, attempt_id="a", at=_AT), "e.json")

    assert not list(tmp_path.rglob("*.json"))


def test_emit_writes_the_event_by_default(tmp_path):
    uri = _memo_uri(f"file://{tmp_path}/r")
    blob_sink.emit_to_blob(
        uri,
        "mr.Run.abc",
        console.started(
            uri,
            attempt_id="a1",
            at=_AT,
            where=runtime.RuntimeContext("k8s", {"pod_name": "pod-x"}),
        ),
        "e.json",
    )

    written = json.loads(next(tmp_path.rglob("e.json")).read_text())
    assert written["event"] == "started"
    assert written["runtime"] == "k8s"
    assert written["where"] == {"pod_name": "pod-x"}
    assert written["invocation_key"] == "pipe/pkg.mod:fn/hash123"


def test_emit_never_raises_on_an_unwritable_destination():
    """An event that cannot be written must not fail the invocation it describes."""
    with blob_sink.CONSOLE_REMOTE_EVENTS.set_local(True):
        blob_sink.emit_to_blob(
            "not-a-uri-at-all", "mr.Run.abc", console.Event(event="started"), "e.json"
        )


def test_finished_distinguishes_success_from_error():
    uri = _memo_uri("file:///r")

    assert console.finished(uri, attempt_id="a", at=_AT, was_error=False)["event"] == "completed"
    assert console.finished(uri, attempt_id="a", at=_AT, was_error=True)["event"] == "failed"
