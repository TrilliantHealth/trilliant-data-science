from datetime import date, datetime, timedelta, timezone

from thds.humenc import temporal


def test_bimonthly_cycle_boundaries() -> None:
    start, length = temporal.bimonthly_cycle(date(2026, 7, 17))
    assert start == datetime(2026, 7, 1, tzinfo=timezone.utc)
    assert length == timedelta(days=62)

    start, length = temporal.bimonthly_cycle(date(2026, 1, 1))
    assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert length == timedelta(days=59)

    start, length = temporal.bimonthly_cycle(date(2026, 9, 1))
    assert start == datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert length == timedelta(days=61)


def test_encode_temporal_is_humenc_shaped() -> None:
    for _ in range(100):
        s = temporal.encode_temporal()
        assert "." in s, f"expected humenc dot separator in {s!r}"
        assert s[0].isupper(), f"expected leading uppercase in {s!r}"


def test_same_day_shares_first_letter() -> None:
    now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    first_letters = {temporal.encode_temporal(now=now)[0] for _ in range(50)}
    assert len(first_letters) <= 2


def test_different_days_differ() -> None:
    early = {
        temporal.encode_temporal(now=datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc))[:5]
        for _ in range(20)
    }
    late = {
        temporal.encode_temporal(now=datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc))[:5]
        for _ in range(20)
    }
    assert early.isdisjoint(late)


def test_custom_cycle() -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    length = timedelta(days=7)
    early = {
        temporal.encode_temporal(
            now=start + timedelta(hours=1),
            cycle_start=start,
            cycle_length=length,
        )[:5]
        for _ in range(20)
    }
    late = {
        temporal.encode_temporal(
            now=start + timedelta(days=6),
            cycle_start=start,
            cycle_length=length,
        )[:5]
        for _ in range(20)
    }
    assert early.isdisjoint(late)


def test_more_time_bits_finer_granularity() -> None:
    """With 10 time bits over a 61-day cycle, two timestamps 2 hours
    apart should usually land in different buckets."""
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    length = timedelta(days=61)
    t1 = start + timedelta(days=30, hours=0)
    t2 = start + timedelta(days=30, hours=2)
    words_t1 = {
        temporal.encode_temporal(
            time_bits=10,
            now=t1,
            cycle_start=start,
            cycle_length=length,
        ).split(".")[0]
        for _ in range(20)
    }
    words_t2 = {
        temporal.encode_temporal(
            time_bits=10,
            now=t2,
            cycle_start=start,
            cycle_length=length,
        ).split(".")[0]
        for _ in range(20)
    }
    assert words_t1.isdisjoint(words_t2)


def test_custom_epoch_months() -> None:
    quarterly = (1, 4, 7, 10)
    start, length = temporal.bimonthly_cycle(date(2026, 7, 1), quarterly)
    assert start == datetime(2026, 7, 1, tzinfo=timezone.utc)
    assert length == timedelta(days=92)
