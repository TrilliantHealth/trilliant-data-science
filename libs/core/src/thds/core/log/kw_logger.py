"""A logger which allows passing of arbitrary keyword arguments to the end of a logger call,
such that that context gets embedded directly into the output in one way or another.
"""

import contextlib
import logging
import logging.config
from copy import copy
from typing import Any, Dict, MutableMapping, Optional

from .. import config
from ..stack_context import StackContext

LOGLEVEL = config.item("thds.core.log.level", logging.INFO, parse=logging.getLevelName)
_LOGGING_KWARGS = ("exc_info", "stack_info", "stacklevel", "extra")
# These are the officially accepted keyword-arguments for a call to
# log something with the logger. Anything passed with these names
# should be passed through directly - anything else can be passed through
# to the keyword formatter.

TH_REC_CTXT = "th_context"
# this names a nested dict on some LogRecords that contains things we
# want to log. It is usable as a field specifier in log format strings


class _THContext(Dict[str, Any]):
    def __str__(self):
        return ",".join(map("(%s=%s)".__mod__, self.items())) if self else "()"


_LOG_CONTEXT: StackContext[_THContext] = StackContext("TH_LOG_CONTEXT", _THContext())


@contextlib.contextmanager
def logger_context(**kwargs):
    """Put some key-value pairs into the keyword-based logger context."""
    with _LOG_CONTEXT.set(_THContext(_LOG_CONTEXT(), **kwargs)):
        yield


def _embed_th_context_in_extra_kw(kwargs: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Extracts the key-value pairs embedded via `logger_context, overlays those with
    keyword arguments to the logger, and embeds them all in the logger's "extra" dictionary.
    """
    th_context = _LOG_CONTEXT()
    th_kwargs = [k for k in kwargs if k not in _LOGGING_KWARGS]
    if th_kwargs:
        th_context = copy(th_context)
        th_context.update((k, kwargs.pop(k)) for k in th_kwargs)
    extra = kwargs["extra"] = kwargs.get("extra", dict())
    extra[TH_REC_CTXT] = th_context
    return kwargs


class KwLogger(logging.LoggerAdapter):
    """Allows logging of extra keyword arguments straight through without
    needing an "extras" dictionary.
    """

    def process(self, msg, kwargs):
        return msg, _embed_th_context_in_extra_kw(kwargs)


def th_keyvals_from_record(record: logging.LogRecord) -> Optional[Dict[str, Any]]:
    """Extracts the key-value pairs embedded via `logger_context` or keyword arguments from a LogRecord."""
    return getattr(record, TH_REC_CTXT, None)


def getLogger(name: Optional[str] = None) -> logging.LoggerAdapter:
    """Using this Logger Adapter will allow you to pass key/value context at the end of
    your logging statements, e.g. `logger.info("my message", key1=value1, key2=value2)`.
    Provided that you haven't configured your own logging format, this module will do so for you,
    ensuring that these contextual key-value pairs render in your log messages. To ensure their presence
    when configuring logging yourself, just put a "%(th_context)s" format specifier somewhere in your
    log message format.
    """
    logger = logging.getLogger(name)
    if logger.level == logging.NOTSET:
        logger.setLevel(LOGLEVEL())
    return KwLogger(logging.getLogger(name), dict())


def make_th_formatters_safe(logger: logging.Logger):
    """Non-adapted loggers may still run into our root format string,
    which expects _TH_REC_CTXT to be present on every LogRecord.
    This will patch one in to any logs making it to our configured formatter.
    """
    for handler in logger.handlers:
        formatter = handler.formatter
        if formatter and hasattr(formatter, "_style") and TH_REC_CTXT in formatter._style._fmt:
            fmt_msg = formatter.formatMessage

            def wrapper_formatMessage(record: logging.LogRecord):
                if None is getattr(record, TH_REC_CTXT, None):
                    setattr(record, TH_REC_CTXT, _LOG_CONTEXT())
                return fmt_msg(record)  # noqa: B023

            setattr(formatter, "formatMessage", wrapper_formatMessage)  # noqa: B010
