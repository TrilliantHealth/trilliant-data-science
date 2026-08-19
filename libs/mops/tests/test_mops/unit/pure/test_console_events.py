import datetime as dt

from thds.mops.pure.tools.console import events

_MEMO_URI = "file:///root/mops2-mpf/pipe/pkg.mod--fn/hash123"
_HIT_AT = dt.datetime(2026, 8, 12, 15, 0, tzinfo=dt.timezone.utc)


def test_a_memoized_event_is_terminal_at_the_moment_it_was_served():
    event = events.memoized(_MEMO_URI, at=_HIT_AT)

    assert event["event"] == "completed"
    assert event["was_memoized"] is True
    assert event["at"] == _HIT_AT.isoformat()


def test_a_memoized_event_carries_the_original_computation():
    """The reused result's own account of when it ran, kept apart from the main
    timestamps so a reader places the hit at serve time unless asked otherwise."""
    event = events.memoized(
        _MEMO_URI,
        at=_HIT_AT,
        invoked_at="2026-08-01T10:00:00+00:00",
        started_at="2026-08-01T10:05:00+00:00",
        ended_at="2026-08-01T10:20:00+00:00",
        run_name="2026-08-01/mr.Original.abc",
    )

    assert event["original_invoked_at"] == "2026-08-01T10:00:00+00:00"
    assert event["original_started_at"] == "2026-08-01T10:05:00+00:00"
    assert event["original_ended_at"] == "2026-08-01T10:20:00+00:00"
    assert event["original_run_name"] == "2026-08-01/mr.Original.abc"
    assert "invoked_at" not in event
    assert "started_at" not in event


def test_a_memoized_exception_is_a_failure():
    """Memoizing exceptions is a real mode, and a cache-served failure is still a
    failure."""
    assert events.memoized(_MEMO_URI, at=_HIT_AT, was_error=True)["event"] == "failed"
