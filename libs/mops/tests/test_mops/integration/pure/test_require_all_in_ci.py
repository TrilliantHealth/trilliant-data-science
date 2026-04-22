from datetime import datetime

import pytest

from thds.mops import pure
from thds.mops.pure.core.memo.results import RequiredResultNotFound
from thds.mops.testing.results import require_all_in_ci

from ...config import TEST_TMP_URI


@pure.memoize_in(TEST_TMP_URI)
def _trivial(x: str) -> str:
    return x


def test_raises_on_cache_miss_in_ci(monkeypatch):
    """The fixture/context manager should raise RequiredResultNotFound in CI on cache miss."""
    monkeypatch.setenv("CI", "true")
    with pytest.raises(RequiredResultNotFound):
        with require_all_in_ci():
            _trivial(datetime.now().isoformat())


def test_allows_computation_outside_ci(monkeypatch):
    """Outside CI, memoized functions should compute fresh results without error."""
    monkeypatch.delenv("CI", raising=False)
    with require_all_in_ci():
        result = _trivial(datetime.now().isoformat())
    assert isinstance(result, str)
