import os

from thds.core import journalist as core_journalist
from thds.mops.pure.core.entry import journalist


def _reset(monkeypatch):
    monkeypatch.delenv(journalist.JOURNALIST_INTERVAL_ENV, raising=False)
    journalist.LOG_INTERVAL.set_global(0.0)
    core_journalist._ACTIVE = None


def test_disabled_by_default(monkeypatch):
    _reset(monkeypatch)
    with journalist.maybe_journalist("mops2-mpf", ()):
        pass


def test_activates_and_pops_env_when_positive(monkeypatch):
    _reset(monkeypatch)
    journalist.LOG_INTERVAL.set_global(5.0)
    monkeypatch.setenv(journalist.JOURNALIST_INTERVAL_ENV, "5.0")

    with journalist.maybe_journalist("mops2-mpf", ()):
        assert journalist.JOURNALIST_INTERVAL_ENV not in os.environ


def test_non_positive_is_off(monkeypatch):
    _reset(monkeypatch)
    journalist.LOG_INTERVAL.set_global(-1.0)
    with journalist.maybe_journalist("mops2-mpf", ()):
        assert journalist.LOG_INTERVAL() == -1.0  # not touched when off


def test_unparseable_parses_to_zero(caplog):
    assert journalist._parse_log_interval("nonsense") == 0.0


def test_parses_numeric_strings():
    assert journalist._parse_log_interval("7.5") == 7.5
    assert journalist._parse_log_interval(3) == 3.0


def test_label_falls_back_without_args(monkeypatch):
    _reset(monkeypatch)
    assert journalist._label("mops2-mpf", ()) == "mops2-mpf"


def test_label_falls_back_on_unparseable_memo_uri(monkeypatch):
    _reset(monkeypatch)
    assert journalist._label("mops2-mpf", ("not-a-valid-uri",)) == "mops2-mpf"
