"""The pytest plugin, run the way a project's CI runs it: a fresh pytest process per case."""

import typing as ty
from pathlib import Path

import pytest

# a memoized function whose marker file records whether its body ran.
_MEMOIZED_FUNCTION = """
from pathlib import Path

from thds.mops import pure


@pure.memoize_in("file://{blob_root}")
def _memoized(marker: str) -> str:
    Path(marker).write_text("ran")
    return marker

"""
# called while the module is imported, which pytest does during conftest loading or collection.
_CALL_AT_IMPORT = _MEMOIZED_FUNCTION + '_memoized("{marker}")\n'
_CALL_IN_TEST_BODY = (
    _MEMOIZED_FUNCTION + 'def test_calls_memoized() -> None:\n    _memoized("{marker}")\n'
)
_TRIVIAL_TEST = "def test_trivial() -> None:\n    pass\n"

# where pytest imports the call from: a test module and a non-initial conftest during collection,
# and an initial conftest before pytest_configure.
_LAYOUTS = {
    "test-module": {"test_at_import.py": _CALL_AT_IMPORT + _TRIVIAL_TEST},
    "initial-conftest": {"conftest.py": _CALL_AT_IMPORT, "test_trivial.py": _TRIVIAL_TEST},
    "collected-conftest": {"sub/conftest.py": _CALL_AT_IMPORT, "sub/test_trivial.py": _TRIVIAL_TEST},
}
# how pytest reports the import failing: a usage error for an initial conftest, which fails before
# the session starts, and an interrupted collection for the others.
_FAILURE_EXIT_CODES = {
    "test-module": pytest.ExitCode.INTERRUPTED,
    "initial-conftest": pytest.ExitCode.USAGE_ERROR,
    "collected-conftest": pytest.ExitCode.INTERRUPTED,
}


def _write_files(pytester: pytest.Pytester, files: ty.Mapping[str, str]) -> Path:
    """Writes each template under the pytester root; returns the marker path the templates share."""
    marker = pytester.path / "ran.marker"
    for relpath, template in files.items():
        path = pytester.path / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(template.format(blob_root=pytester.path / "blob-root", marker=marker))

    return marker


@pytest.mark.parametrize("layout", list(_LAYOUTS))
def test_in_ci_a_mops_call_at_import_requires_a_result(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    monkeypatch.setenv("CI", "true")
    marker = _write_files(pytester, _LAYOUTS[layout])

    result = pytester.runpytest_subprocess()

    assert result.ret == _FAILURE_EXIT_CODES[layout]
    assert "RequiredResultNotFound" in result.stdout.str() + result.stderr.str()
    assert not marker.exists(), "the function was invoked"


@pytest.mark.parametrize("layout", list(_LAYOUTS))
def test_outside_ci_a_mops_call_at_import_computes(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    monkeypatch.delenv("CI", raising=False)
    marker = _write_files(pytester, _LAYOUTS[layout])

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)
    assert marker.exists()


def test_without_the_plugin_ci_computes(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CI", "true")
    marker = _write_files(pytester, _LAYOUTS["test-module"])

    result = pytester.runpytest_subprocess("-p", "no:pytest-mops")

    result.assert_outcomes(passed=1)
    assert marker.exists()


def test_overriding_the_fixture_lifts_the_requirement_for_tests(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CI", "true")
    pytester.makeconftest(
        """
        import typing as ty

        import pytest

        @pytest.fixture(scope="session", autouse=True)
        def _mops_require_all_in_ci() -> ty.Iterator[None]:
            yield
        """
    )
    marker = _write_files(pytester, {"test_in_body.py": _CALL_IN_TEST_BODY})

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)
    assert marker.exists()
