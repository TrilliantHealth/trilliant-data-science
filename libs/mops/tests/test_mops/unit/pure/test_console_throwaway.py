from pathlib import Path

import pytest

from thds.mops.pure.tools.console import throwaway, writer


def _unmark(monkeypatch) -> None:
    """Clear every signal that this is a test run.

    `PYTEST_CURRENT_TEST` cannot be cleared from a fixture - pytest re-sets it for each
    test after its fixtures have run - so this is called from the test body instead.
    """
    for name in throwaway._ENV_VARS:
        monkeypatch.setenv(name, "")


@pytest.mark.parametrize("variable", ["PYTEST_VERSION", "PYTEST_CURRENT_TEST", "CI"])
def test_each_signal_is_enough_on_its_own(monkeypatch, variable):
    """`PYTEST_VERSION` only exists from pytest 8 and `PYTEST_CURRENT_TEST` is unset
    outside a running test, so neither alone covers a whole suite."""
    _unmark(monkeypatch)
    monkeypatch.setenv(variable, "1")

    assert throwaway.here()


def test_an_ordinary_process_is_not_throwaway(monkeypatch):
    _unmark(monkeypatch)

    assert not throwaway.here()


def test_a_caller_can_declare_its_own_runs_disposable(monkeypatch):
    """For work that is not run under anything this would recognise."""
    _unmark(monkeypatch)

    with throwaway.THROWAWAY_RUNS.set_local(True):
        assert throwaway.here()


def test_a_real_name_is_left_alone(monkeypatch):
    _unmark(monkeypatch)

    assert throwaway.suffixed("events") == "events"


def test_a_throwaway_name_is_marked(monkeypatch):
    monkeypatch.setenv("CI", "true")

    assert throwaway.suffixed("events") == "events-throwaway"


def test_local_events_move_aside_too(monkeypatch):
    """A project using mops inside its own tests should not have to pick its real runs out
    from among them."""
    monkeypatch.setenv("CI", "true")

    with writer.CONSOLE_EVENTS_DIR.set_local(Path(".mops/events")):
        assert writer.events_dir().parent.parent.name == "events-throwaway"


def test_local_events_stay_beside_a_configured_location(monkeypatch):
    """The leaf is marked, not the root, so this does not land back under the default."""
    monkeypatch.setenv("CI", "true")

    with writer.CONSOLE_EVENTS_DIR.set_local(Path("/somewhere/of/my/own")):
        assert str(writer.events_dir()).startswith("/somewhere/of/my/own-throwaway/")


def test_a_disabled_console_is_left_disabled(monkeypatch):
    """The empty path is how local events are turned off; marking it would raise."""
    monkeypatch.setenv("CI", "true")

    with writer.CONSOLE_EVENTS_DIR.set_local(Path("")):
        assert not str(writer.events_dir()).startswith("-throwaway")
