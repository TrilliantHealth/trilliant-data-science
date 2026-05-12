"""End-to-end test that a @pure.magic-decorated recursive function actually re-enters
mops on each recursive call, so each unique argument tuple gets its own memo URI.

This is the property the docs/fibonacci.py quickstart example relies on. Without it,
the first call to a recursive @pure.magic function bypasses memoization for its
descendants, which silently breaks the entire memoization story for recursive code.

The test asserts on the *number of memo URIs created on disk* — checking only the
return value would silently pass even if recursion bypassed memoization entirely,
because the function still computes the correct result inline.
"""

from datetime import datetime
from pathlib import Path

import pytest

from thds.mops import pure

from ...config import TEST_TMP_URI

_PIPELINE_ID = f"test/recursive-magic/{datetime.utcnow().isoformat()}"


@pytest.fixture
def clear_magic():
    pure._magic.api._MAGIC_CONFIG = pure._magic.sauce.new_config()  # type: ignore
    yield
    pure._magic.api._MAGIC_CONFIG = pure._magic.sauce.new_config()  # type: ignore


# Decorated at module scope so we exercise the non-__main__ import-resolution path.
# Both paths must work; a separate unit test covers __main__ via the
# `_LOCAL_MAIN_FUNCTIONS` registry.
@pure.magic(blob_root=TEST_TMP_URI, pipeline_id=_PIPELINE_ID)
def _fib(n: int) -> int:
    if n <= 1:
        return n
    return _fib(n - 1) + _fib(n - 2)


def _memo_dir_for_fib() -> Path | None:
    """Resolve the on-disk directory that mops uses for `_fib`'s memo URIs, or None
    if blob_root is not a `file://` URI (in which case skip the count assertion)."""
    if not TEST_TMP_URI.startswith("file://"):
        return None
    # Strip scheme; the remainder is what the local file blob store treats as a path,
    # which may be absolute or relative to cwd. Path() handles both.
    root = Path(TEST_TMP_URI.removeprefix("file://"))
    return root / "mops2-mpf" / _PIPELINE_ID / f"{__name__}--_fib"


def test_recursive_magic_function_memoizes_each_unique_argument(clear_magic):
    # fib(5) recursively touches 5, 4, 3, 2, 1, 0 — six unique arg tuples. Each
    # should produce its own memo URI on disk.
    assert _fib(5) == 5

    memo_dir = _memo_dir_for_fib()
    if memo_dir is None:
        pytest.skip("blob_root is not local; cannot count memo URIs on disk")

    memo_uris = [p for p in memo_dir.iterdir() if p.is_dir()]
    assert len(memo_uris) == 6, (
        f"Expected 6 memo URIs under {memo_dir} (one per unique fib argument 0..5), "
        f"got {len(memo_uris)}: {[p.name for p in memo_uris]}"
    )

    # Re-running with the cache populated should not create new memo URIs.
    assert _fib(5) == 5
    assert len([p for p in memo_dir.iterdir() if p.is_dir()]) == 6
