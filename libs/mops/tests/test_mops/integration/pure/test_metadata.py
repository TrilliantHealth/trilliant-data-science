import logging
import re
import typing as ty

from thds.mops.pure.core import metadata, uris
from thds.mops.pure.pickling._pickle import read_metadata_and_object

from ._util import adls_shim


def extract_memo_uris(caplog_records) -> ty.Iterator[str]:
    found_at_least_one = False
    memo_uri = ""
    # extract memo uri from log
    for record in caplog_records:
        # this regex matches all printable characters excluding SPACE.
        # this helps us avoid matching any of the color ANSI codes or other such stuff.
        m = re.search(r"Invoking ([!-~]+://[!-~]+)", record.msg)
        if m:
            memo_uri = m.group(1)
            found_at_least_one = True
            yield memo_uri

    assert found_at_least_one, (
        "Could not find memo URI in logs - are we missing 'new invocation for...'?"
    )


@adls_shim
def makes_some_metadata(foo: str) -> int:
    return int(foo)


def test_metadata_is_available_in_result(caplog):
    with caplog.at_level(logging.INFO):
        with (
            metadata.INVOKER_CODE_VERSION.set_local(
                "foobarVersion",
            ),
            metadata.INVOKED_BY.set_local(
                "testing-user",
            ),
        ):
            # run it three times so we have some fun stuff to look at with mops-inspect.
            assert 3 == makes_some_metadata("3")
            assert 4 == makes_some_metadata("4")
            assert 5 == makes_some_metadata("5")

    for memo_uri in extract_memo_uris(caplog.records):
        meta, _rv = read_metadata_and_object("metadata-and-rv", memo_uri + "/result")

        assert meta, "Metadata should be found in the result!"

        assert meta.invoked_by == "testing-user"
        assert 1 > meta.remote_wall_minutes > 0
        assert 1 > meta.result_wall_minutes > 0
        assert meta.remote_started_at < meta.remote_ended_at
        assert meta.invoked_at < meta.remote_started_at
        assert meta.invoker_code_version == "foobarVersion"
        assert meta.remote_code_version == meta.invoker_code_version
        # for local runs, the invoker code version is reused.


@adls_shim
def returns_quickly(x: int) -> int:
    return x * 2


def test_result_metadata_contains_diagnostics_when_threshold_is_zero(caplog, monkeypatch):
    """Result metadata should include environment diagnostics when threshold is 0."""
    # Must use env var to propagate config to subprocess
    monkeypatch.setenv("MOPS_RESULT_DIAGNOSTICS_THRESHOLD_SECONDS", "0")

    with caplog.at_level(logging.INFO):
        result = returns_quickly(21)
        assert result == 42

    memo_uris = list(extract_memo_uris(caplog.records))
    assert memo_uris, "Should have captured at least one memo URI"

    memo_uri = memo_uris[0]
    bs = uris.lookup_blob_store(memo_uri)

    # read the result file to get the run_id from metadata
    meta, _rv = read_metadata_and_object("result", bs.join(memo_uri, "result"))
    assert meta is not None, "Result should have metadata"
    assert meta.run_id, "Metadata should have a run_id"

    # construct the metadata filename and read it
    metadata_uri = bs.join(memo_uri, f"result-metadata-{meta.run_id}.txt")
    assert bs.exists(metadata_uri), f"Result metadata file should exist at {metadata_uri}"

    metadata_content = bs.getfile(metadata_uri).read_text()

    # verify standard metadata fields are present
    assert "invoked-at=" in metadata_content
    assert "run-id=" in metadata_content

    # verify diagnostic sections are present (no exception/traceback for successful results)
    assert "=== Environment ===" in metadata_content
    assert "python_version=" in metadata_content
    assert "platform=" in metadata_content

    assert "=== Installed Packages ===" in metadata_content
    assert "thds.mops==" in metadata_content or "thds-mops==" in metadata_content


def test_format_extra_metadata():
    """Unit test for format_extra_metadata function."""
    # Empty dict produces no output
    assert metadata.format_extra_metadata({}) == ""

    # Single field
    result = metadata.format_extra_metadata({"foo": "bar"})
    assert "=== Extra Metadata ===" in result
    assert "foo=bar" in result

    # Multiple fields are sorted
    result = metadata.format_extra_metadata({"zebra": "z", "alpha": "a", "beta": "b"})
    assert "=== Extra Metadata ===" in result
    lines = result.strip().split("\n")
    # After strip: header, then sorted fields
    assert lines[0] == "=== Extra Metadata ==="
    assert lines[1] == "alpha=a"
    assert lines[2] == "beta=b"
    assert lines[3] == "zebra=z"


def test_load_metadata_generator_with_empty_config():
    """When config is empty, generator loading returns None."""
    with metadata.EXTRA_METADATA_GENERATOR.set_local(""):
        result = metadata.load_metadata_generator()
    assert result is None


def test_load_metadata_generator_with_invalid_path(caplog):
    """Invalid import path logs warning and returns None."""
    with caplog.at_level(logging.WARNING):
        with metadata.EXTRA_METADATA_GENERATOR.set_local("nonexistent.module.func"):
            result = metadata.load_metadata_generator()
    assert result is None
    assert "Failed to load extra metadata generator" in caplog.text


def test_parse_result_metadata_captures_extra_fields():
    """Extra key=value pairs should be captured in the extra dict."""
    lines = [
        "invoked-at=2026-01-29T17:00:00+00:00",
        "invoked-by=test-user",
        "invoker-code-version=v1.0",
        "invoker-uuid=test-uuid",
        "pipeline-id=test/pipeline",
        "remote-code-version=v1.0",
        "remote-started-at=2026-01-29T17:00:01+00:00",
        "remote-ended-at=2026-01-29T17:00:02+00:00",
        "remote-wall-minutes=0.017",
        "result-wall-minutes=0.033",
        "run-id=2601291700-TestRun",
        "",
        "=== Extra Metadata ===",
        "grafana_logs=https://grafana.example.com/explore?query=foo",
        "k8s_pod_name=my-pod-abc123",
        "k8s_namespace=test-namespace",
        "",
        "=== Environment ===",
        "python_version=3.10.0",
    ]
    result = metadata.parse_result_metadata(lines)

    # Standard fields should be parsed
    assert result.invoked_by == "test-user"
    assert result.run_id == "2601291700-TestRun"

    # Extra fields should be captured (before the === Environment === section)
    assert "grafana_logs" in result.extra
    assert result.extra["grafana_logs"] == "https://grafana.example.com/explore?query=foo"
    assert result.extra["k8s_pod_name"] == "my-pod-abc123"
    assert result.extra["k8s_namespace"] == "test-namespace"

    # Environment section should NOT be included
    assert "python_version" not in result.extra
