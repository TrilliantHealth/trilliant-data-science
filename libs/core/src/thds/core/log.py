"""New and improved logger for Trilliant.
Now you can add keyword arguments to your log statements and they will
get formatted nicely in the logging message. If we ever move to
structured/JSON logging, we can write a useful Formatter for that
scenario as well.
Additionally, you can add additional context (via keyword arguments)
to logs at any time by inserting a logger_context, and this context
will accompany all future logging statements further down the stack,
but not once it has been exited.
Observe:
```
logger = getLogger("FooF")
logger.warning("testing")
# 2022-02-18 10:01:16,825 WARNING  FooF () testing 1
logger.info("testing 2", two=3, eight="nine")
# 2022-02-18 10:01:16,826 info     FooF (two=3,eight=nine) testing 2
with logger_context(App='bat', override='me'):
    logger.info("testing 3", yes='no')
# 2022-02-18 10:01:16,827 info     FooF (App=bat,override=me,yes=no) testing 3
    logger.info("testing 4", override='you')
# 2022-02-18 10:01:16,828 info     FooF (App=bat,override=you) testing 4
logger.info("testing 5")
# 2022-02-18 10:01:16,829 info     FooF () testing 5
```
"""

import contextlib
import logging
import logging.config
import os
import typing as ty
from copy import copy
from typing import Any, Dict, Iterator, MutableMapping, Optional, Tuple

from . import basic_color, config
from .stack_context import StackContext

_LOGLEVEL = config.item("level", logging.INFO, parse=logging.getLevelName)
_LOGLEVELS_FILEPATH = config.item("levels_file", "", parse=lambda s: s.strip())
# see _parse_thds_loglevels_file for format of this file.

_LOGGING_KWARGS = ("exc_info", "stack_info", "stacklevel", "extra")
# These are the officially accepted keyword-arguments for a call to
# log something with the logger. Anything passed with these names
# should be passed through directly - anything else can be passed through
# to the keyword formatter.

_TH_REC_CTXT = "th_context"
# this names a nested dict on some LogRecords that contains things we
# want to log. It is usable as a field specifier in log format strings

MAX_MODULE_NAME_LEN = config.item("max_module_name_len", 40, parse=int)
_MODULE_NAME_FMT_STR = "{compressed_name:" + str(MAX_MODULE_NAME_LEN()) + "}"

_COLOR_LEVEL_MAP = {
    "low": f"{basic_color.fg.BLUE}{{}}{basic_color.fg.RESET}",
    "info": f"{basic_color.fg.GREEN}{{}}{basic_color.fg.RESET}",
    "warning": f"{basic_color.fg.YELLOW}{basic_color.style.BRIGHT}{{}}{basic_color.style.RESET_ALL}",
    "error": f"{basic_color.bg.RED}{basic_color.style.BRIGHT}{{}}{basic_color.style.RESET_ALL}",
    "critical": f"{basic_color.bg.MAGENTA}{basic_color.style.BRIGHT}{{}}{basic_color.style.RESET_ALL}",
}


def _log_level_color(record: logging.LogRecord, base_levelname: str) -> str:
    if record.levelno < logging.INFO:
        return _COLOR_LEVEL_MAP["low"].format(base_levelname.lower())
    elif record.levelno < logging.WARNING:
        return _COLOR_LEVEL_MAP["info"].format(base_levelname.lower())
    elif record.levelno < logging.ERROR:
        return _COLOR_LEVEL_MAP["warning"].format(base_levelname)
    elif record.levelno < logging.CRITICAL:
        return _COLOR_LEVEL_MAP["error"].format(base_levelname)
    return _COLOR_LEVEL_MAP["critical"].format(base_levelname)


class ThdsCompactFormatter(logging.Formatter):
    """This new formatter is more compact than what we had before, and hopefully makes logs a bit more readable overall."""

    @staticmethod
    def format_module_name(name: str) -> str:
        max_module_name_len = MAX_MODULE_NAME_LEN()
        compressed_name = (
            name
            if len(name) <= max_module_name_len
            else name[: max_module_name_len // 2 - 2] + "..." + name[-max_module_name_len // 2 + 1 :]
        )
        assert len(compressed_name) <= max_module_name_len
        return _MODULE_NAME_FMT_STR.format(compressed_name=compressed_name)

    def _format_exception_and_trace(self, record: logging.LogRecord):
        # without the following boilerplate, we would not see exceptions or stack traces
        # get formatted as part of the log output at all.
        formatted = ""
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            formatted += "\n" + record.exc_text
        if record.stack_info:
            formatted += "\n" + self.formatStack(record.stack_info)
        return formatted

    def format(self, record: logging.LogRecord):
        record.message = record.getMessage()

        base_levelname = f"{record.levelname:7}"  # the length of the string 'WARNING'
        levelname = _log_level_color(record, base_levelname)

        th_ctx = getattr(record, _TH_REC_CTXT, None) or tuple()
        short_name = self.format_module_name(record.name)
        formatted = f"{self.formatTime(record)} {levelname}  {short_name} {th_ctx} {record.message}"
        if exc_text := self._format_exception_and_trace(record):
            formatted += exc_text
        return formatted


class _THContext(Dict[str, Any]):
    def __str__(self):
        return ",".join(map("(%s=%s)".__mod__, self.items())) if self else "()"


_LOG_CONTEXT: StackContext[_THContext] = StackContext("TH_LOG_CONTEXT", _THContext())


@contextlib.contextmanager
def logger_context(**kwargs):
    with _LOG_CONTEXT.set(_THContext(_LOG_CONTEXT(), **kwargs)):
        yield


def _embed_th_context_in_extra_kw(kwargs: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    th_context = _LOG_CONTEXT()
    th_kwargs = [k for k in kwargs if k not in _LOGGING_KWARGS]
    if th_kwargs:
        th_context = copy(th_context)
        th_context.update((k, kwargs.pop(k)) for k in th_kwargs)
    extra = kwargs["extra"] = kwargs.get("extra", dict())
    extra[_TH_REC_CTXT] = th_context
    return kwargs


class KwLogger(logging.LoggerAdapter):
    """Allows logging of extra keyword arguments straight through without
    needing an "extras" dictionary.
    """

    def process(self, msg, kwargs):
        return msg, _embed_th_context_in_extra_kw(kwargs)


def getLogger(name: Optional[str] = None) -> logging.LoggerAdapter:
    """Using this Logger Adapter will allow you to pass key/value context at the end of
    your logging statements, e.g. `logger.info("my message", key1=value1, key2=value2)`.
    Provided that you haven't configured your own logging format, this module will do so for you,
    ensuring that these contextual key-value pairs render in your log messages. To ensure their presence
    when configuring logging yourself, just put a "%(th_context)s" format specifier somewhere in your
    log message format.
    """
    return KwLogger(logging.getLogger(name), dict())


def make_th_formatters_safe(logger: logging.Logger):
    """Non-adapted loggers may still run into our root format string,
    which expects _TH_REC_CTXT to be present on every LogRecord.
    This will patch one in to any logs making it to our configured formatter.
    """
    for handler in logger.handlers:
        formatter = handler.formatter
        if formatter and _TH_REC_CTXT in formatter._style._fmt:
            fmt_msg = formatter.formatMessage

            def wrapper_formatMessage(record: logging.LogRecord):
                if None is getattr(record, _TH_REC_CTXT, None):
                    setattr(record, _TH_REC_CTXT, _LOG_CONTEXT())
                return fmt_msg(record)  # noqa: B023

            setattr(formatter, "formatMessage", wrapper_formatMessage)  # noqa: B010


# this is the base of what gets passed to logging.dictConfig.
_BASE_LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"()": ThdsCompactFormatter}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "default"}},
    "root": {
        "handlers": ["console"],
        "level": _LOGLEVEL(),
    },
}


def set_logger_to_console_level(config: dict, logger_name: str, level: int) -> dict:
    if logger_name == "*":
        if level != _LOGLEVEL():
            getLogger(__name__).warning(f"Setting root logger to {logging.getLevelName(level)}")
        return dict(config, root=dict(config["root"], level=level))
    loggers = config.get("loggers") or dict()
    loggers = {**loggers, logger_name: {"level": level, "handlers": ["console"], "propagate": False}}
    # propagate=False means, don't pass this up the chain to loggers
    # matching a subset of our name.  The level is set on the logger,
    # not the handler, but if a logger is set to propagate,then it
    # will pass its message up the chain until it hits propagate=False
    # or the root. And any loggers with the appropriate logging level
    # will emit to any handlers they have configured. So, generally,
    # you want to put handlers at the same level where
    # propagate=False, which is what we do here.
    return dict(config, loggers=loggers)


def _parse_thds_loglevels_file(filepath: str) -> Iterator[Tuple[str, int]]:
    """Example loglevels file:

    ```
    [debug]
    thds.adls.download
    thds.mops.pure.pickle_runner
    thds.nppes.intake.parquet_from_csv

    [warning]
    *
    # the * sets the root logger to warning-and-above. INFO is the default.
    ```

    The last value encountered for any given logger (or the root) will
    override any previous values.
    """
    current_level = _LOGLEVEL()
    if not os.path.exists(filepath):
        return
    with open(filepath) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("[") and line.endswith("]"):
                current_level = getattr(
                    logging, line[1:-1].upper()
                )  # AttributeError means invalid level
                continue
            logger_name = line
            yield logger_name, current_level


class DuplicateFilter:
    """Filters away duplicate log messages.

    Taken from @erb's answer on SO: https://stackoverflow.com/questions/31953272/logging-print-message-only-once
    """

    def __init__(self, logger: ty.Union[logging.Logger, logging.LoggerAdapter]):
        self.msgs: ty.Set[str] = set()
        self.logger = logger.logger if isinstance(logger, logging.LoggerAdapter) else logger

    def filter(self, record: logging.LogRecord):
        msg = str(record.msg)
        is_duplicate = msg in self.msgs
        if not is_duplicate:
            self.msgs.add(msg)
        return not is_duplicate

    def __enter__(self):
        self.logger.addFilter(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.removeFilter(self)


if not logging.getLogger().hasHandlers():
    live_config = _BASE_LOG_CONFIG
    for logger_name, level in _parse_thds_loglevels_file(_LOGLEVELS_FILEPATH()):
        live_config = set_logger_to_console_level(live_config, logger_name, level)
    logging.config.dictConfig(live_config)
    make_th_formatters_safe(logging.getLogger())

    class StartsWithFilter(logging.Filter):
        def __init__(self, startswith: str):
            self.startswith = startswith

        def filter(self, record):
            return not record.name.startswith(self.startswith)

    for noisy_logger in ("py4j.java_gateway", "py4j.clientserver"):  # 11.3, 9.1
        logging.getLogger(noisy_logger).addFilter(StartsWithFilter(noisy_logger))
