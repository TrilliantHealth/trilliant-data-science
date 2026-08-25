"""`peek` answers "what would this call return, without computing" - value on a hit,
`Unmemoized` on a miss, and no invocation, result, exception, or lease written."""

import pickle

import pytest

from thds.mops import pure
from thds.mops.pure.core import file_blob_store
from thds.mops.pure.runner import get_results, simple_shims


def _triple(x: int) -> int:
    return x * 3


def _total(xs: list) -> int:
    return sum(xs)


def _all_files(root) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()} if root.exists() else set()


def test_peek_miss_returns_unmemoized_with_the_calls_memo_uri(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    missed = wand.peek(14)
    assert isinstance(missed, pure.Unmemoized)
    # the peek derived the same memo URI the real call uses.
    assert missed.memo_uri == wand.submit(14).memo_uri


def test_peek_writes_no_invocation_state(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    assert isinstance(wand.peek(14), pure.Unmemoized)
    assert not _all_files(tmp_path), "a peek must not create even the invocation marker"


def test_peek_hit_returns_the_bare_value(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    assert wand(14) == 42
    assert wand.peek(14) == 42
    assert isinstance(wand.peek(15), pure.Unmemoized)  # different args, different key


def test_unmemoized_invoke_computes_and_then_peek_hits(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    missed = wand.peek(14)
    assert isinstance(missed, pure.Unmemoized)
    assert missed.invoke() == 42
    assert wand.peek(14) == 42


def test_unmemoized_refuses_pickling(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    missed = wand.peek(14)
    with pytest.raises(TypeError, match="cannot be serialized"):
        pickle.dumps(missed)


def test_unmemoized_invoke_fills_its_own_uri_outside_the_peeks_context(tmp_path) -> None:
    # the memospace resolved at peek time is carried by the Unmemoized, so a mask that
    # has since been left behind does not move the hole out from under invoke().
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    with pure.pipeline_id_mask("peek-mask"):
        missed = wand.peek(14)
    assert isinstance(missed, pure.Unmemoized)
    assert "/peek-mask/" in missed.memo_uri

    assert missed.invoke() == 42  # outside the mask entirely

    with pure.pipeline_id_mask("peek-mask"):
        assert wand.peek(14) == 42, "invoke() filled a different memo URI than it reported"


def test_unmemoized_invoke_refuses_when_the_arguments_changed(tmp_path) -> None:
    # the memospace is carried, but the arguments hash cannot be: invoke() re-serializes,
    # because that is what registers deferred uploads. Args mutated in between would fill
    # a different hole than the one reported, so it refuses instead.
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_total)
    xs = [1, 2, 3]
    missed = wand.peek(xs)
    assert isinstance(missed, pure.Unmemoized)

    xs.append(100)
    with pytest.raises(pure.UnmemoizedContextLost, match="would not fill the hole"):
        missed.invoke()

    # refused *before* computing: neither URI was filled, so nothing was spent and no
    # result exists under the mutated arguments either.
    assert isinstance(wand.peek([1, 2, 3]), pure.Unmemoized), "the peeked URI was filled anyway"
    assert isinstance(wand.peek(xs), pure.Unmemoized), "a result was computed at the wrong URI"


def test_unmemoized_invoke_uses_the_shim_in_force_when_it_is_called(tmp_path) -> None:
    # only the memo location is fixed at peek time; the shim - and so the runtime the
    # call lands on - is whatever is in force at invoke(). A handle peeked under off()
    # must therefore still be invocable once a runner is available again.
    used: list[str] = []

    def counting_shim_builder(f, args, kwargs):
        def shim(argv):
            used.append("custom")
            return simple_shims.samethread_shim(argv)

        return shim

    magic = pure.magic(blob_root=f"file://{tmp_path}", pipeline_id="shim-later")(_triple)
    with magic.off():
        missed = magic.peek(14)
    assert isinstance(missed, pure.Unmemoized)
    assert not used, "a peek must not run any shim"

    with magic.shim(counting_shim_builder):
        assert missed.invoke() == 42
    assert used == ["custom"], "invoke() did not use the shim in force at call time"
    assert magic.peek(14) == 42, "invoke() filled a different memo URI than it reported"


class _Shareable:
    """`.shared()` registers by identity in a weak-keyed registry, so the object has to
    be weak-referenceable - a bare dict is not. `__weakref__` because `__slots__`
    otherwise removes exactly that."""

    __slots__ = ("contents", "__weakref__")

    def __init__(self, contents: dict):
        self.contents = contents


def _shared_arg_user(shared: _Shareable, key: int) -> int:
    return shared.contents[key]


def test_peek_writes_only_shared_argument_bytes(tmp_path) -> None:
    # a peek writes no invocation/result/exception/lease state, but `.shared()` arguments
    # upload their content-addressed bytes inline while serializing, and serializing is
    # how the memo key is derived. Isolate the control root to see exactly what appears.
    blobs, control = tmp_path / "blobs", tmp_path / "control"
    magic = pure.magic("samethread", blob_root=f"file://{blobs}", pipeline_id="shared-peek").shared(
        "shared"
    )(_shared_arg_user)

    with file_blob_store.MOPS_ROOT.set_local(control):
        assert isinstance(magic.peek(_Shareable({1: 10}), 1), pure.Unmemoized)

    assert not _all_files(blobs), "a peek wrote invocation or memo state"
    written = _all_files(control)
    assert written, "expected the shared argument's content-addressed bytes"
    assert all("sha256-b64-addressed" in f for f in written), written


def test_peek_hit_reports_no_cache_hit(tmp_path, monkeypatch) -> None:
    # a peek hit is not a cache hit this run made. Reporting one would let polling
    # invent history: N peeks would read as N cache hits that never happened.
    events, summaries = [], []
    monkeypatch.setattr(get_results.console, "memoized", lambda *a, **kw: events.append(a[:1]))
    monkeypatch.setattr(
        get_results.run_summary, "log_function_execution", lambda *a, **kw: summaries.append(a[:3])
    )
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    wand(14)  # a real call, which does report

    events.clear()
    summaries.clear()
    assert wand.peek(14) == 42
    assert wand.peek(14) == 42
    assert not events, "a peek hit emitted a console cache-hit event"
    assert not summaries, "a peek hit wrote a run-summary entry"


def test_peek_miss_does_not_raise_under_require_all(tmp_path) -> None:
    # require_all makes a *call* fail rather than compute; a peek's whole purpose
    # is to report the miss, so it must answer Unmemoized instead of raising.
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_triple)
    with pure.results.require_all("peeks must not trip this"):
        assert isinstance(wand.peek(14), pure.Unmemoized)


def test_magic_decorator_also_peeks(tmp_path) -> None:
    magic_fn = pure.magic("samethread", blob_root=f"file://{tmp_path}")(_quadruple)
    assert isinstance(magic_fn.peek(4), pure.Unmemoized)
    assert magic_fn(4) == 16
    assert magic_fn.peek(4) == 16


def _quadruple(x: int) -> int:
    return x * 4
