from datetime import datetime, timezone

from thds.mops.pure.pickling.remote import _generate_run_id


def test_generate_run_id_format():
    """Verify run_id format: YYMMDDHHmm-TwoWords"""
    started_at = datetime(2026, 1, 27, 15, 23, 0, tzinfo=timezone.utc)
    run_id = _generate_run_id(started_at)

    # Format: YYMMDDHHmm-TwoWords
    assert run_id.startswith("2601271523-")
    # Two humenc words (from 2 bytes) - verify it's alphanumeric after the dash
    suffix = run_id.split("-", 1)[1]
    assert suffix.isalpha()
    assert len(suffix) > 0
