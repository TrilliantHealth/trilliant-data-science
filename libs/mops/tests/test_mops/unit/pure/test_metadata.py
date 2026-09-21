from thds.core import log
from thds.mops.pure.core import metadata


def test_log_context_metadata_records_whatever_tagged_the_invocation():
    with log.logger_context(mops_fn="mod--fn", mops_args="AbC123", mqclass="train"):
        assert metadata.log_context_metadata() == {
            "mops_fn": "mod--fn",
            "mops_args": "AbC123",
            "mqclass": "train",
        }


def test_log_context_metadata_drops_values_the_metadata_format_cannot_hold():
    """`key=value` lines are parsed by splitting, so a space or a newline in a value would
    be read back as something else entirely."""
    with log.logger_context(spaced="two words", multiline="a\nb", empty="", fine="ok"):
        assert metadata.log_context_metadata() == {"fine": "ok"}


def test_log_context_metadata_sees_only_the_scopes_entered_when_it_is_called():
    """The capture site is the point of the design: read it before entering scopes of your
    own and it describes the work, not the reader."""
    with log.logger_context(mops_args="AbC123"):
        launched = metadata.log_context_metadata()
        with log.logger_context(upload="mops-putfile"):
            assert metadata.log_context_metadata() == {
                "mops_args": "AbC123",
                "upload": "mops-putfile",
            }

    assert launched == {"mops_args": "AbC123"}


def test_parse_result_metadata_keeps_a_context_key_that_prefixes_a_real_flag():
    """Extra metadata reaches the parser as `--<key>=<value>`, so `remote` sits in front of
    `--remote-code-version` and three others. It is an extra, not an ambiguous flag."""
    parsed = metadata.parse_result_metadata(
        [
            "invoked-by=sam@laptop",
            "invoker-code-version=20260101.0000-abc1234",
            "invoked-at=2026-01-01T00:00:00+00:00",
            "pipeline-id=nightly",
            "remote-started-at=2026-01-01T00:01:00+00:00",
            "remote-ended-at=2026-01-01T00:02:00+00:00",
            "=== Extra Metadata ===",
            "remote=nightly",
            "mops_args=AbC123",
        ]
    )
    assert parsed.extra["remote"] == "nightly"
    assert parsed.extra["mops_args"] == "AbC123"
