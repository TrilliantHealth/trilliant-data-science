import logging

from thds.core.timer import timer


def test_timer(caplog):
    caplog.set_level(logging.INFO)

    @timer
    def foo():
        x = 1 + 1
        return x

    foo()
    for record in caplog.records:
        assert record.levelname == "INFO"
    assert "Starting 'foo'" in caplog.records[0].message
    assert "Finished 'foo' in" in caplog.records[1].message
