import logging
import re
import typing as ty

from thds.mops.pure.core import metadata
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

    assert (
        found_at_least_one
    ), "Could not find memo URI in logs - are we missing 'new invocation for...'?"


@adls_shim
def makes_some_metadata(foo: str) -> int:
    return int(foo)


def test_metadata_is_available_in_result(caplog):
    with caplog.at_level(logging.INFO):
        with metadata.INVOKER_CODE_VERSION.set_local("foobarVersion",), metadata.INVOKED_BY.set_local(
            "testing-user",
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
