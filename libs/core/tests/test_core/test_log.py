from thds.core import log


def test_compressed_module_name():
    mname = log.ThdsCompactFormatter.format_module_name(
        "very_very_very_long_module_name_that_is_extremely_long"
    )
    assert "very_very_very_lon...t_is_extremely_long" == mname
    assert len(mname) == log.MAX_MODULE_NAME_LEN()


def test_short_module_name():
    mname = log.ThdsCompactFormatter.format_module_name("short")
    assert mname == "short" + " " * (log.MAX_MODULE_NAME_LEN() - len("short"))
    assert len(mname) == log.MAX_MODULE_NAME_LEN()


def test_compressed_module_name_fmt():
    fmt = log.ThdsCompactFormatter()
    assert fmt.format(
        log.logging.LogRecord(
            "very_very_very_long_module_name_that_is_extremely_long",
            log.logging.INFO,
            "pathname",
            1,
            "i have things to say",
            (),
            None,
        )
    ).endswith("info     very_very_very_lon...t_is_extremely_long () i have things to say")


def test_keyword_context_formatting(caplog):
    with log.logger_context(foo="bar", baz=7), caplog.at_level(log.logging.INFO):
        logger = log.getLogger("test")
        logger.info("i have said certain things to you")
        caplog.records[0].message.endswith("(foo='bar',baz=7) i have said certain things to you")
