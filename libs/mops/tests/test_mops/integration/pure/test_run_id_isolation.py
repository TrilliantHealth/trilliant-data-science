"""Integration test for run_id isolation of output paths.

Verifies that multiple executions of the same function write outputs to
separate directories (keyed by run_id), preventing race conditions where
concurrent runs could overwrite each other's outputs.

Uses local filesystem to avoid ADLS caching complications - the run_id
mechanism is blob-store agnostic.
"""

import logging
from datetime import datetime

import pytest

from thds.core.files import path_from_uri
from thds.core.source import Source, from_file
from thds.mops import pure, tempdir

from .test_metadata import extract_memo_uris

# Use local filesystem - avoids ADLS caching that interferes with result deletion
_LOCAL_BLOB_ROOT = "file://./.mops-test"


# Function must be at module level for mops to find it when unpickling
@pure.magic(
    blob_root=_LOCAL_BLOB_ROOT, pipeline_id=f"test/run-id-isolation/{datetime.utcnow().isoformat()}"
)
def _produces_source_output() -> Source:
    output_file = tempdir() / "output.txt"
    output_file.write_text("some output content")
    return from_file(output_file)


@pytest.mark.integration
def test_multiple_runs_produce_outputs_in_separate_run_id_directories(caplog):
    """Verify that re-running a function writes outputs to a new run_id directory."""
    # First run
    with caplog.at_level(logging.INFO):
        source1 = _produces_source_output()
    uri1 = source1.uri

    # Find and delete the result file so mops will run the function again
    memo_uri = list(extract_memo_uris(caplog.records))[0]
    result_path = path_from_uri(memo_uri) / "result"
    result_path.unlink()

    caplog.clear()

    # Second run - should execute fresh since result file is gone
    with caplog.at_level(logging.INFO):
        source2 = _produces_source_output()
    uri2 = source2.uri

    # Verify outputs are in different directories (different run_ids in path)
    # URI structure: .../args_hash/run_id/filename
    assert uri1 != uri2, "Output URIs should differ between runs"

    parts1 = uri1.rsplit("/", 2)
    parts2 = uri2.rsplit("/", 2)
    # parts = [base_path_through_args_hash, run_id, filename]

    assert parts1[0] == parts2[0], "Same base path up to args_hash"
    assert parts1[1] != parts2[1], "Different run_ids"
    assert parts1[2] == parts2[2], "Same filename"
