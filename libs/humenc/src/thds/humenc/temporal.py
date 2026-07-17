"""Time-aware humenc slug generation.

Encodes a coarse timestamp into the top bits of the payload so
that wordybin's alphabetically-sorted dictionary produces a first
word whose leading letter advances through the alphabet across the
cycle. A debugging signal for roughly when a slug was minted.

The two knobs are `time_bits` (how many of the top bits carry
temporal signal) and the cycle (how long before the time bits roll
over). More time bits = finer granularity but fewer random bits in
the word-pair. The suffix bytes are always fully random.
"""

import secrets
from collections import abc
from datetime import date, datetime, timedelta, timezone

from thds import humenc

_BIMONTHLY = (1, 3, 5, 7, 9, 11)
_DEFAULT_TIME_BITS = 6


def bimonthly_cycle(d: date, epoch_months: abc.Sequence[int] = _BIMONTHLY) -> tuple[datetime, timedelta]:
    """Return (cycle_start, cycle_length) for the bimonthly epoch
    containing `d`. `epoch_months` lists the month numbers (1-12)
    that start each epoch."""
    candidates = [date(d.year, m, 1) for m in epoch_months if date(d.year, m, 1) <= d]
    candidates.append(date(d.year - 1, epoch_months[-1], 1))
    start = max(candidates)

    next_candidates = [date(start.year, m, 1) for m in epoch_months if date(start.year, m, 1) > start]
    if not next_candidates:
        next_start = date(start.year + 1, epoch_months[0], 1)
    else:
        next_start = min(next_candidates)

    start_dt = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    length = timedelta(days=(next_start - start).days)
    return start_dt, length


def encode_temporal(
    num_bytes: int = 8,
    *,
    word_bytes: int = 2,
    time_bits: int = _DEFAULT_TIME_BITS,
    now: datetime | None = None,
    cycle_start: datetime | None = None,
    cycle_length: timedelta | None = None,
) -> str:
    """Mint a humenc string with a time-sorted first word.

    `time_bits` controls how many of the top bits encode temporal
    position within the cycle. The remaining bits (and all suffix
    bytes) are random.

    When `cycle_start`/`cycle_length` are omitted, defaults to
    bimonthly epochs (Jan 1, Mar 1, May 1, Jul 1, Sep 1, Nov 1).
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if cycle_start is None or cycle_length is None:
        cycle_start, cycle_length = bimonthly_cycle(now.date())

    elapsed = (now - cycle_start).total_seconds()
    cycle_seconds = cycle_length.total_seconds()
    fraction = min(elapsed / cycle_seconds, 1.0)
    time_val = int(fraction * ((1 << time_bits) - 1))

    random_bits_count = (num_bytes * 8) - time_bits
    raw = ((time_val << random_bits_count) | secrets.randbits(random_bits_count)).to_bytes(
        num_bytes, "big"
    )
    return humenc.encode(raw, num_bytes=word_bytes)
