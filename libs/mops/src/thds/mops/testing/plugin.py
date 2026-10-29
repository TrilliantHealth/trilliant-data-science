"""Pytest plugin that automatically enforces memoized results exist in CI environments.

Registered as a pytest11 entry point so it activates for any project with mops installed.
The requirement covers the initial conftest imports, collection and the tests themselves;
docs/testing.adoc says how to narrow or switch it off.
For tests that need a custom error message, use the `require_all_in_ci` context manager directly.
"""

import typing as ty

import pytest

from thds.mops.testing.results import require_all_in_ci


@pytest.hookimpl(hookwrapper=True)
def pytest_load_initial_conftests() -> ty.Iterator[None]:
    # pytest imports these conftests before pytest_configure, so no later hook or fixture can cover them.
    with require_all_in_ci():
        yield


@pytest.hookimpl(hookwrapper=True)
def pytest_collection() -> ty.Iterator[None]:
    # collection imports every test module, and the conftests below the initial ones.
    with require_all_in_ci():
        yield


@pytest.fixture(scope="session", autouse=True)
def _mops_require_all_in_ci() -> ty.Iterator[None]:
    with require_all_in_ci():
        yield
