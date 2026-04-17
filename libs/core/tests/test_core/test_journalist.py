import logging

from thds.core import journalist


def _reset_active():
    journalist._ACTIVE = None


def test_single_entry_claims_active_slot():
    _reset_active()
    j = journalist.Journalist("solo", interval=100.0, sample_interval=100.0)
    if not j._enabled:  # psutil missing - feature not applicable
        return

    with j as active:
        assert journalist._ACTIVE is active
        assert active._enabled
    assert journalist._ACTIVE is None


def test_nested_entry_becomes_noop():
    _reset_active()
    outer = journalist.Journalist("outer", interval=100.0, sample_interval=100.0)
    inner = journalist.Journalist("inner", interval=100.0, sample_interval=100.0)
    if not outer._enabled:
        return

    with outer:
        assert journalist._ACTIVE is outer
        with inner:
            assert journalist._ACTIVE is outer
            assert not inner._enabled
            assert inner._thread is None
        # exiting the no-op must not clear the active slot
        assert journalist._ACTIVE is outer
    assert journalist._ACTIVE is None


def test_exit_logs_final_summary(caplog):
    _reset_active()
    # Large `interval` so the sample loop's periodic log never fires inside the
    # with-block. The only log emitted should be the final-summary on __exit__.
    j = journalist.Journalist("summary-test", interval=100.0, sample_interval=0.05)
    if not j._enabled:
        return

    import time

    with caplog.at_level(logging.INFO, logger="thds.core.journalist"):
        with j:
            time.sleep(0.3)

    lines = [r.getMessage() for r in caplog.records if "summary-test" in r.getMessage()]
    assert len(lines) == 1
    assert "MEM" in lines[0]
    assert "CPU" in lines[0]
