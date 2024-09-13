import logging

from thds.core import log
from thds.core.log.kw_formatter import MAX_MODULE_NAME_LEN


def test_compressed_module_name():
    mname = log.ThdsCompactFormatter.format_module_name(
        "very_very_very_long_module_name_that_is_extremely_long"
    )
    assert "very_very_very_lon...t_is_extremely_long" == mname
    assert len(mname) == MAX_MODULE_NAME_LEN()


def test_short_module_name():
    mname = log.ThdsCompactFormatter.format_module_name("short")
    assert mname == "short" + " " * (MAX_MODULE_NAME_LEN() - len("short"))
    assert len(mname) == MAX_MODULE_NAME_LEN()


def test_compressed_module_name_fmt():
    fmt = log.ThdsCompactFormatter()
    msg = fmt.format(
        logging.LogRecord(
            "very_very_very_long_module_name_that_is_extremely_long",
            logging.INFO,
            "pathname",
            1,
            "i have things to say",
            (),
            None,
        )
    )

    assert msg.endswith(" very_very_very_lon...t_is_extremely_long () i have things to say")
    assert "info   " in msg


def test_keyword_context_formatting(caplog):
    with log.logger_context(foo="bar", baz=7), caplog.at_level(logging.INFO):
        logger = log.getLogger("test")
        logger.info("i have said certain things to you")
        caplog.records[0].message.endswith("(foo='bar',baz=7) i have said certain things to you")


def test_levels_dont_break(capsys, caplog):
    logger = log.getLogger("test")
    with capsys.disabled(), caplog.at_level(logging.DEBUG):
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL):
            logger.log(level, f"this is a {level} message")
