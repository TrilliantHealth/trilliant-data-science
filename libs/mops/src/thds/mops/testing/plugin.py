"""Pytest plugin that automatically enforces memoized results exist in CI environments.

Registered as a pytest11 entry point so it activates for any project with mops installed.
For tests that need a custom error message, use the `require_all_in_ci` context manager directly.
"""

import typing as ty

import pytest

from thds.mops.testing.results import _require_all_in_ci


@pytest.fixture(scope="session", autouse=True)
def _mops_require_all_in_ci() -> ty.Iterator[None]:
    yield from _require_all_in_ci()
