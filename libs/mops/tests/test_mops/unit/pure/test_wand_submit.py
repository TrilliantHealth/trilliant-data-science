"""`pure.magic.wand` returns a callable object exposing both blocking call and submit()."""

from thds.mops import pure


def _double(x: int) -> int:
    return x * 2


def test_wand_blocking_call_still_works(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_double)
    assert wand(21) == 42


def test_wand_exposes_submit_returning_pfuture(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_double)
    fut = wand.submit(21)
    assert hasattr(fut, "done") and hasattr(fut, "result")
    assert fut.result() == 42


def test_wand_submit_memo_hit_resolves(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_double)
    assert wand(21) == 42  # populate memo
    fut = wand.submit(21)  # should resolve from memo, not recompute
    assert fut.done()
    assert fut.result() == 42


# Guards that a function which calls itself recursively through a Wand does not
# introduce a re-dispatch cycle: each nested call is a fresh mops invocation that
# computes its own result and returns it to the caller directly.
def _recursive_sum(n: int) -> int:
    if n <= 0:
        return 0
    return n + _recursive_sum(n - 1)


def test_wand_does_not_break_remote_bypass(tmp_path) -> None:
    wand = pure.magic.wand("samethread", blob_root=f"file://{tmp_path}")(_recursive_sum)
    assert wand(5) == 15
    assert wand.submit(5).result() == 15


def test_wand_off_returns_bare_function_without_submit() -> None:
    # 'off' (and None-from-config-off) build no runner, so the bare function comes back
    # unchanged: callable, but no `.submit()`. The overloads type this path as the
    # `Wand | Callable` union so a static checker won't let a caller assume `.submit()`.
    off = pure.magic.wand("off")(_double)
    assert off(21) == 42
    assert not hasattr(off, "submit")
