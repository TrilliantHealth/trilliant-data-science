import concurrent.futures
import pickle

import pytest

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


def test_done_callback_fires_when_the_invocation_failed():
    """A FAILED tuple-future must still fire the user's done-callback.

    There is no metadata tuple to unpack on a failure, so the capture step raises -
    and because concurrent.futures logs a raising callback and moves on to the next,
    that used to eat the user's callback entirely. Anyone awaiting completion via
    add_done_callback (rather than a blocking .result()) then waited forever for a
    query that had already failed."""
    pending: "concurrent.futures.Future[tuple[int, object]]" = concurrent.futures.Future()
    mf: MopsFuture[int] = MopsFuture.from_tuple_future(pending, memo_uri="m")  # type: ignore[arg-type]

    seen: list[MopsFuture[int]] = []
    mf.add_done_callback(seen.append)  # type: ignore[arg-type]

    pending.set_exception(ValueError("the pod OOMed"))

    assert len(seen) == 1, "the done-callback was dropped for a failed invocation"
    # The failure is not swallowed - it reaches the caller through the future it's handed.
    with pytest.raises(ValueError, match="the pod OOMed"):
        seen[0].result()
    assert mf.result_metadata is None


def test_done_callback_fires_when_the_invocation_was_cancelled():
    """Same contract for a CANCELLED tuple-future, which raises CancelledError rather
    than the invocation's own exception when its result is read."""
    pending: "concurrent.futures.Future[tuple[int, object]]" = concurrent.futures.Future()
    mf: MopsFuture[int] = MopsFuture.from_tuple_future(pending, memo_uri="m")  # type: ignore[arg-type]

    seen: list[MopsFuture[int]] = []
    mf.add_done_callback(seen.append)  # type: ignore[arg-type]

    assert pending.cancel()

    assert len(seen) == 1, "the done-callback was dropped for a cancelled invocation"
    with pytest.raises(concurrent.futures.CancelledError):
        seen[0].result()


def test_failed_invocation_settles_a_reified_future():
    """The consequence that matters for async callers: `reify_future` chains onto
    add_done_callback, so a dropped callback left the reified future PENDING forever.
    A failed invocation must settle it (with the exception), never hang it."""
    pending: "concurrent.futures.Future[tuple[int, object]]" = concurrent.futures.Future()
    mf: MopsFuture[int] = MopsFuture.from_tuple_future(pending, memo_uri="m")  # type: ignore[arg-type]
    reified = futures.reify_future(mf)

    pending.set_exception(ValueError("evicted"))

    assert reified.done(), "a failed invocation left its reified future unresolved"
    with pytest.raises(ValueError, match="evicted"):
        reified.result(timeout=0)


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


@pytest.mark.parametrize(
    "invoc_type,emits",
    [
        pytest.param("memoized", True, id="memoized-hits-are-reported"),
        pytest.param("awaited", True, id="awaited-is-a-memoized-hit-this-run-waited-for"),
        pytest.param("invoked", False, id="invoked-work-is-reported-by-its-remote"),
    ],
)
def test_reused_results_emit_a_console_event(monkeypatch, invoc_type, emits):
    """Nothing else will ever report a result this run did not compute: no remote of ours
    ran it, and its summary stays on this machine."""
    reported = []
    monkeypatch.setattr(get_results.console, "memoized", lambda *a, **k: reported.append(k))

    get_results.unwrap_value_or_error(
        lambda type_hint, uri: (None, "THE_VALUE"),
        None,
        "runner_prefix",
        (),
        "adls://x/memo",
        get_results.ResultAndInvocationType(
            memo.results.Success(value_uri="adls://x/value"), invoc_type
        ),
    )

    assert bool(reported) == emits
