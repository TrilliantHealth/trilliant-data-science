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
logger.info("testing")
# 2022-02-18 10:01:16,825 - FooF - INFO - testing 1
logger.info("testing 2", two=3, eight="nine")
# 2022-02-18 10:01:16,826 - FooF - INFO - (two=3),(eight=nine) - testing 2
with logger_context(App='bat', override='me'):
    logger.info("testing 3", yes='no')
# 2022-02-18 10:01:16,827 - FooF - INFO - (App=bat),(override=me),(yes=no) - testing 3
    logger.info("testing 4", override='you')
# 2022-02-18 10:01:16,828 - FooF - INFO - (App=bat),(override=you) - testing 4
logger.info("testing 5")
# 2022-02-18 10:01:16,829 - FooF - INFO - testing 5
```
"""
import contextlib
import logging
import logging.config
import os
from copy import copy
from typing import Any, Dict, MutableMapping, Optional

from .stack_context import StackContext

_LOGLEVEL = os.environ.get("LOGLEVEL", logging.INFO)
_LOGGERS_TO_DEBUG = os.environ.get("THDS_DEBUG_LOGGERS", "").split(";")
# set this env var to (for example)
# `thds.adls.download;thds.mops.remote.pickle_runner` to get debug
# logs for those two modules.

_LOGGING_KWARGS = ("exc_info", "stack_info", "stacklevel", "extra")
# These are the officially accepted keyword-arguments for a call to
# log something with the logger. Anything passed with these names
# should be passed through directly - anything else can be passed through
# to the keyword formatter.

_TH_REC_CTXT = "th_context"
# this names a nested dict on some LogRecords that contains things we
# want to log. It is usable as a field specifier in log format strings

TH_DEFAULT_LOG_FORMAT = f"%(asctime)s - %(name)s - %(levelname)s - %({_TH_REC_CTXT})s - %(message)s"
# Default log format used when not configuring one's own logging,
# including the th_context key-value pairs

# this is what gets passed to logging.dictConfig.
_DEFAULT_LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"format": TH_DEFAULT_LOG_FORMAT}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "default"}},
    "root": {
        "handlers": ["console"],
        "level": _LOGLEVEL,
    },
}


def set_logger_to_console_debug(config: dict, logger_name: str) -> dict:
    loggers = config.get("loggers") or dict()
    loggers = {**loggers, logger_name: {"level": "DEBUG", "handlers": ["console"], "propagate": False}}
    # propogate=False means, don't pass this up the chain to loggers
    # matching a subset of our name.  The level is set on the logger,
    # not the handler, but if a logger is set to propogate,then it
    # will pass its message up the chain until it hits propogate=False
    # or the root. And any loggers with the appropriate logging level
    # will emit to any handlers they have configured. So, generally,
    # you want to put handlers at the same level where
    # propogate=False, which is what we do here.
    return dict(config, loggers=loggers)


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


class NoJavaFilter(logging.Filter):
    # Filters out noisy py4j logs when logging on a Spark cluster.
    def filter(self, record):
        return not record.name.startswith("py4j.java_gateway")


if not logging.getLogger().hasHandlers():
    config = _DEFAULT_LOG_CONFIG
    for logger_name in _LOGGERS_TO_DEBUG:
        config = set_logger_to_console_debug(config, logger_name)
    logging.config.dictConfig(config)
    make_th_formatters_safe(logging.getLogger())
    logging.getLogger("py4j.java_gateway").addFilter(NoJavaFilter())
