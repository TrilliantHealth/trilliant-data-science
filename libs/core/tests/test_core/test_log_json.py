import logging

from thds.core.log.json_formatter import ThdsJsonFormatter


def test_json_format(caplog):
    formatter = ThdsJsonFormatter()

    record = logging.LogRecord(
        "name",
        logging.INFO,
        "tests/test_core/test_log_json.py",
        1,
        "This is a test message",
        (),
        None,
    )
    record.th_context = dict(foo="bar", baz="qux")

    fmtted_msg = formatter.format(record)
    print(fmtted_msg)
    assert fmtted_msg.endswith(
        '"level": "INFO", "module": "test_log_json", "msg": "This is a test message", "foo": "bar", "baz": "qux"}'
    )
