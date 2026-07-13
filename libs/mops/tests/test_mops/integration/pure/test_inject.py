"""Injected precomputed results must be indistinguishable from computed ones
(aside from the extra-metadata marker) - written by real shims, read back through
the normal memoized path."""

from datetime import datetime

import pytest

from thds.mops import pure
from thds.mops.pure.pickling import inject

from ...config import TEST_TMP_URI

_PIPELINE_ID = f"test/inject/{datetime.utcnow().isoformat()}"


@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE_ID)
def expensive_add(a: int, b: int) -> int:
    raise AssertionError("must never actually run during these tests")


def test_injecting_shim_builder_writes_result_that_future_calls_memo_hit():
    def make_value(func, args, kwargs):
        assert func.__name__ == "expensive_add"
        return lambda: args[0] + args[1] + 1000  # deliberately NOT what the code does

    with expensive_add.shim(inject.shim_builder(make_value)):
        assert expensive_add(1, 2) == 1003  # injected value returned via the normal read path

    # now with the ordinary shim: the injected result memo-hits; the function body never runs.
    assert expensive_add(1, 2) == 1003


def test_capturing_shim_then_imperative_write_result():
    with expensive_add.shim(inject.capturing_shim):
        with pytest.raises(inject.MemoUriCapture) as excinfo:
            expensive_add(7, 8)

    capture = excinfo.value
    assert "expensive_add/" in capture.memo_uri
    inject.write_result(capture.memo_uri, 8000, *capture.metadata_args)

    assert expensive_add(7, 8) == 8000


def test_injected_result_carries_precomputed_marker():
    with expensive_add.shim(inject.shim_builder(lambda f, a, kw: lambda: 42)):
        fut = expensive_add.submit(20, 22)
        assert fut.result() == 42

    md = fut.result_metadata
    assert md is not None
    assert md.extra.get("precomputed_result") == "true"


def test_injection_only_happens_on_memo_miss():
    with expensive_add.shim(inject.shim_builder(lambda f, a, kw: lambda: 99)):
        assert expensive_add(50, 60) == 99  # miss: injected

    def exploding_make_value(func, args, kwargs):
        raise AssertionError("builder must not be consulted for a memoized call")

    with expensive_add.shim(inject.shim_builder(exploding_make_value)):
        assert expensive_add(50, 60) == 99  # memoized: shim builder never invoked
