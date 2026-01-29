import logging

import pytest

from thds.mops.pure.core import uris
from thds.mops.pure.pickling._pickle import read_metadata_and_object

from ._util import adls_shim
from .test_metadata import extract_memo_uris


def func_that_raises(a: int):
    raise ValueError(f"{a} is just no good at all!!")


@adls_shim
def func_that_calls_other_func_that_raises(a: int):
    return func_that_raises(a)


def test_that_remote_exceptions_can_be_reraised_locally():
    with pytest.raises(ValueError):
        func_that_calls_other_func_that_raises(2)


def test_exception_metadata_contains_diagnostics(caplog):
    """Exception metadata should include diagnostics for debugging."""
    with caplog.at_level(logging.INFO):
        with pytest.raises(ValueError):
            func_that_calls_other_func_that_raises(99)

    memo_uris = list(extract_memo_uris(caplog.records))
    assert memo_uris, "Should have captured at least one memo URI"

    memo_uri = memo_uris[0]
    bs = uris.lookup_blob_store(memo_uri)

    # read the exception file to get the run_id from metadata
    meta, _exc = read_metadata_and_object("exception", bs.join(memo_uri, "exception"))
    assert meta is not None, "Exception should have metadata"
    assert meta.run_id, "Metadata should have a run_id"

    # construct the metadata filename and read it
    metadata_uri = bs.join(memo_uri, f"exception-metadata-{meta.run_id}.txt")
    assert bs.exists(metadata_uri), f"Exception metadata file should exist at {metadata_uri}"

    metadata_content = bs.getfile(metadata_uri).read_text()

    # verify standard metadata fields are present
    assert "invoked-at=" in metadata_content
    assert "invoked-by=" in metadata_content
    assert "run-id=" in metadata_content

    # verify diagnostic sections are present
    assert "=== Exception ===" in metadata_content
    assert "type=" in metadata_content
    assert "is just no good at all" in metadata_content

    assert "=== Stack Trace ===" in metadata_content
    assert "Traceback (most recent call last):" in metadata_content
    assert "ValueError" in metadata_content

    assert "=== Environment ===" in metadata_content
    assert "python_version=" in metadata_content
    assert "platform=" in metadata_content

    assert "=== Installed Packages ===" in metadata_content
    # at minimum, mops itself should be in the package list (name may have dot or hyphen)
    assert "thds.mops==" in metadata_content or "thds-mops==" in metadata_content
