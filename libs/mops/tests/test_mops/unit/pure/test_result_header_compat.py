"""A result's metadata header is read by every process that hits its memo, including ones
running an older mops. What mops writes must stay readable by those readers."""

from datetime import datetime, timezone

from thds.mops.pure.core import metadata
from thds.mops.pure.pickling import remote

from . import metadata_reader_3_33

_INVOKED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
_LOG_CONTEXT = {
    "remote": "nightly",  # prefixes --remote-code-version and three others: ambiguous
    "invoked": "yes",  # prefixes --invoked-by and --invoked-at: ambiguous
    "pipeline": "not-the-pipeline",  # prefixes only --pipeline-id: an abbreviation of it
    "run": "not-the-run",  # prefixes only --run-id
    "mops_args": "AbC123",
}


def _header_lines(log_context: dict, extra: dict) -> list:
    """What a result blob starts with, from the same properties `return_value` writes."""
    channel = remote.ResultExcWithMetadataChannel(
        fs=None,  # type: ignore[arg-type]
        dumper=None,  # type: ignore[arg-type]
        call_id="memo://unit/header",
        invocation_metadata=metadata.InvocationMetadata(
            invoked_by="sam@laptop",
            invoker_code_version="20260101.0000-abc1234",
            invoked_at=_INVOKED_AT,
            invoker_uuid="",
            pipeline_id="nightly-2026-01",
            console_run_name="",
        ),
        started_at=_INVOKED_AT,
        run_id="2601010000-SkirtBus",
        extra_metadata=extra,
        log_context=log_context,
    )
    return (channel._metadata_header + channel._extra_metadata_content).decode("utf-8").split("\n")


def test_a_3_33_reader_parses_a_header_whose_log_context_collides_with_its_flags():
    parsed = metadata_reader_3_33.parse_result_metadata(_header_lines(_LOG_CONTEXT, {"k8s_pod": "p-1"}))

    assert parsed["pipeline_id"] == "nightly-2026-01"
    assert parsed["run_id"] == "2601010000-SkirtBus"
    assert parsed["extra"] == {"k8s_pod": "p-1"}


def test_the_current_reader_returns_the_log_context_among_the_extras():
    parsed = metadata.parse_result_metadata(_header_lines(_LOG_CONTEXT, {"k8s_pod": "p-1"}))

    assert parsed.pipeline_id == "nightly-2026-01"
    assert parsed.run_id == "2601010000-SkirtBus"
    assert parsed.extra == {**_LOG_CONTEXT, "k8s_pod": "p-1"}


def test_caller_extra_metadata_wins_over_the_log_context_on_a_shared_key():
    parsed = metadata.parse_result_metadata(
        _header_lines({"job": "from-context"}, {"job": "from-caller"})
    )

    assert parsed.extra["job"] == "from-caller"


def test_a_log_context_key_named_like_a_field_does_not_change_the_field():
    parsed = metadata.parse_result_metadata(_header_lines({"pipeline_id": "wrong"}, {}))

    assert parsed.pipeline_id == "nightly-2026-01"
