import concurrent.futures
import pickle

from thds.core import futures
from thds.mops import pure
from thds.mops.pure._futures import MopsFuture
from thds.mops.pure.core import memo
from thds.mops.pure.runner import get_results


def test_delegates_and_carries_memo_uri():
    inner = futures.resolved(42)
    f = MopsFuture(inner, memo_uri="adls://x/mops2-mpf/p/fn/hash")
    assert f.memo_uri == "adls://x/mops2-mpf/p/fn/hash"
    assert f.result_metadata is None
    assert f.done() is True
    assert f.result() == 42
    assert f.exception() is None


def test_set_result_metadata_then_readable():
    f = MopsFuture(futures.resolved("v"), memo_uri="m")
    assert f.result_metadata is None
    f.set_result_metadata("METADATA_SENTINEL")  # type: ignore[arg-type]
    assert f.result_metadata == "METADATA_SENTINEL"


def test_picklable_before_and_after_metadata():
    f = MopsFuture(futures.resolved(7), memo_uri="m")
    back = pickle.loads(pickle.dumps(f))
    assert back.memo_uri == "m"
    assert back.result() == 7
    f.set_result_metadata("MD")  # type: ignore[arg-type]
    back2 = pickle.loads(pickle.dumps(f))
    assert back2.result_metadata == "MD"
    assert back2.memo_uri == "m"


def test_unwrap_returns_value_and_metadata(monkeypatch):
    # get_meta_and_result returns (metadata, value); unwrap must now surface both.
    sentinel_md = object()

    def fake_get_meta_and_result(type_hint, uri):
        return sentinel_md, "THE_VALUE"

    success = memo.results.Success(value_uri="adls://x/value")
    result_and_itype = get_results.ResultAndInvocationType(success, "memoized")
    value, md = get_results.unwrap_value_or_error(
        fake_get_meta_and_result,
        None,  # run_directory
        "runner_prefix",  # runner_prefix
        (),  # args_kwargs_uris
        "adls://x/memo",  # memo_uri
        result_and_itype,
    )
    assert value == "THE_VALUE"
    assert md is sentinel_md


def test_from_tuple_future_splits_value_and_metadata():
    """from_tuple_future splits (value, metadata) on .result() and stores metadata."""
    sentinel_md = object()  # stand-in for Optional[ResultMetadata]
    tuple_future = futures.resolved((99, sentinel_md))
    mf: MopsFuture[int] = MopsFuture.from_tuple_future(tuple_future, memo_uri="m")  # type: ignore[arg-type]
    assert mf.result_metadata is None  # not yet resolved
    assert mf.result() == 99
    assert mf.result_metadata is sentinel_md


def test_add_done_callback_does_not_block_on_pending_tuple_future():
    """add_done_callback must register WITHOUT forcing the (still-pending) tuple
    future to resolve. The regression made add_done_callback resolve the inner
    LazyFuture eagerly, blocking the caller for the full underlying computation."""
    pending: "concurrent.futures.Future[tuple[int, object]]" = concurrent.futures.Future()
    mf: MopsFuture[int] = MopsFuture.from_tuple_future(pending, memo_uri="m")  # type: ignore[arg-type]

    seen: list[MopsFuture[int]] = []
    # this call must return promptly; it must NOT block on pending.result().
    mf.add_done_callback(seen.append)  # type: ignore[arg-type]

    assert mf.done() is False  # underlying future still pending
    assert seen == []  # callback has not fired yet

    sentinel_md = object()
    pending.set_result((123, sentinel_md))  # now resolve it

    assert len(seen) == 1
    fired = seen[0]
    assert fired.result() == 123  # callback gets a VALUE-yielding future, not the tuple
    assert mf.result() == 123
    assert mf.result_metadata is sentinel_md


def test_result_metadata_populated_via_done_callback():
    """result_metadata is populated when resolution is reached through the
    done-callback path (not just through .result())."""
    pending: "concurrent.futures.Future[tuple[str, str]]" = concurrent.futures.Future()
    mf: MopsFuture[str] = MopsFuture.from_tuple_future(pending, memo_uri="m")  # type: ignore[arg-type]
    mf.add_done_callback(lambda _f: None)  # type: ignore[arg-type]
    assert mf.result_metadata is None

    pending.set_result(("v", "MD"))
    assert mf.result_metadata == "MD"


def test_from_tuple_future_picklable():
    """MopsFuture built via from_tuple_future remains picklable (LazyFuture inner)."""
    sentinel_md = "METADATA_SENTINEL"
    tuple_future = futures.resolved(("hello", sentinel_md))
    mf: MopsFuture[str] = MopsFuture.from_tuple_future(tuple_future, memo_uri="m")  # type: ignore[arg-type]
    # pickle before resolving
    mf2: MopsFuture[str] = pickle.loads(pickle.dumps(mf))
    assert mf2.memo_uri == "m"
    assert mf2.result_metadata is None
    assert mf2.result() == "hello"
    assert mf2.result_metadata == "METADATA_SENTINEL"


def _trivial_add(x: int, y: int) -> int:
    return x + y


def test_submit_returns_mops_future_with_memo_uri_and_metadata(tmp_path):
    """Submitting a @pure.magic function returns a MopsFuture; both a first run and a
    memo hit populate .memo_uri and .result_metadata, and both runs share the same URI."""
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_trivial_add)

    fut1 = wand.submit(3, 4)
    assert isinstance(fut1, MopsFuture)
    assert fut1.result() == 7
    assert fut1.memo_uri  # non-empty
    assert fut1.result_metadata is not None

    # second submit with same args => memo hit
    fut2 = wand.submit(3, 4)
    assert isinstance(fut2, MopsFuture)
    assert fut2.result() == 7
    assert fut2.memo_uri == fut1.memo_uri
    assert fut2.result_metadata is not None
