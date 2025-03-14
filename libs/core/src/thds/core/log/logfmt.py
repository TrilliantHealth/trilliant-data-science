# MIT License

# Copyright (c) 2022 Joshua Taylor Eppinette

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# flake8: noqa

import io
import logging
import numbers
import traceback
from contextlib import closing
from types import TracebackType
from typing import Dict, List, Optional, Tuple, Type, cast

ExcInfo = Tuple[Type[BaseException], BaseException, TracebackType]

# Reserved log record attributes cannot be overwritten. They
# will not be included in the formatted log.
#
# https://docs.python.org/3/library/logging.html#logrecord-attributes
RESERVED: Tuple[str, ...] = (
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "taskName",
    "thread",
    "threadName",
)


class Logfmter(logging.Formatter):
    @classmethod
    def format_string(cls, value: str) -> str:
        """
        Process the provided string with any necessary quoting and/or escaping.
        """
        needs_dquote_escaping = '"' in value
        # needs_newline_escaping = "\n" in value
        needs_quoting = " " in value or "=" in value

        if needs_dquote_escaping:
            value = value.replace('"', '\\"')

        # if needs_newline_escaping:
        #     value = value.replace("\n", "\\n")

        if needs_quoting:
            value = '"{}"'.format(value)

        return value if value else '""'

    @classmethod
    def format_value(cls, value) -> str:
        """
        Map the provided value to the proper logfmt formatted string.
        """
        if value is None:
            return ""
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, numbers.Number):
            return str(value)

        return cls.format_string(str(value))

    @classmethod
    def format_exc_info(cls, exc_info: ExcInfo) -> str:
        """
        Format the provided exc_info into a logfmt formatted string.

        This function should only be used to format exceptions which are
        currently being handled. Not with those exceptions which are
        manually passed into the logger. For example:

            try:
                raise Exception()
            except Exception:
                logging.exception()
        """
        _type, exc, tb = exc_info

        with closing(io.StringIO()) as sio:
            traceback.print_exception(_type, exc, tb, None, sio)
            value = sio.getvalue()

        # Tracebacks have a single trailing newline that we don't need.
        value = value.rstrip("\n")

        return cls.format_string(value)

    @classmethod
    def format_params(cls, params: dict) -> str:
        """
        Return a string representing the logfmt formatted parameters.
        """
        return " ".join(["{}={}".format(key, cls.format_value(value)) for key, value in params.items()])

    @classmethod
    def normalize_key(cls, key: str) -> str:
        """
        Return a string whereby any spaces are converted to underscores and
        newlines are escaped.

        If the provided key is empty, then return a single underscore. This
        function is used to prevent any logfmt parameters from having invalid keys.

        As a choice of implementation, we normalize any keys instead of raising an
        exception to prevent raising exceptions during logging. The goal is to never
        impede logging. This is especially important when logging in exception handlers.
        """
        if not key:
            return "_"

        return key.replace(" ", "_").replace("\n", "\\n")

    @classmethod
    def get_extra(cls, record: logging.LogRecord) -> dict:
        """
        Return a dictionary of logger extra parameters by filtering any reserved keys.
        """
        return {
            cls.normalize_key(key): value
            for key, value in record.__dict__.items()
            if key not in RESERVED
        }

    def __init__(
        self,
        keys: List[str] = ["at"],
        mapping: Dict[str, str] = {"at": "levelname"},
        datefmt: Optional[str] = None,
    ):
        self.keys = [self.normalize_key(key) for key in keys]
        self.mapping = {self.normalize_key(key): value for key, value in mapping.items()}
        self.datefmt = datefmt

    def format_key_value(self, record: logging.LogRecord, key: str, value) -> str:
        """This is net-new code that makes the formatter more extensible."""
        return f"{key}={self.format_value(value)}"

    def format(self, record: logging.LogRecord) -> str:
        # If the 'asctime' attribute will be used, then generate it.
        if "asctime" in self.keys or "asctime" in self.mapping.values():
            record.asctime = self.formatTime(record, self.datefmt)

        if isinstance(record.msg, dict):
            params = {self.normalize_key(key): value for key, value in record.msg.items()}
        else:
            params = {"msg": record.getMessage()}

        params.update(self.get_extra(record))

        tokens = []

        # Add the initial tokens from the provided list of default keys.
        #
        # This supports parameters which should be included in every log message. The
        # values for these keys must already exist on the log record. If they are
        # available under a different attribute name, then the formatter's mapping will
        # be used to lookup these attributes. e.g. 'at' from 'levelname'
        for key in self.keys:

            attribute = key

            # If there is a mapping for this key's attribute, then use it to lookup
            # the key's value.
            if key in self.mapping:
                attribute = self.mapping[key]

            # If this key is in params, then skip it, because it was manually passed in
            # will be added via the params system.
            if attribute in params:
                continue

            # If the attribute doesn't exist on the log record, then skip it.
            if not hasattr(record, attribute):
                continue

            value = getattr(record, attribute)

            tokens.append(self.format_key_value(record, key, value))

        formatted_params = self.format_params(params)
        if formatted_params:
            tokens.append(formatted_params)

        if record.exc_info:
            # Cast exc_info to its not null variant to make mypy happy.
            exc_info = cast(ExcInfo, record.exc_info)

            tokens.append("exc_info={}".format(self.format_exc_info(exc_info)))

        return " ".join(tokens)


# all of the above is _almost_ identical (minus format_key_value) to the raw source from
# https://github.com/jteppinette/python-logfmter
#
# what follows is our slight modifications

from .. import ansi_esc
from .kw_formatter import ThdsCompactFormatter
from .kw_logger import TH_REC_CTXT

# similar to what's in kw_formatter, but does not use background colors
# since those don't seem to translate well in k8s/Grafana:
_COLOR_LEVEL_MAP = {
    "low": f"{ansi_esc.fg.BLUE}{{}}{ansi_esc.fg.RESET}",
    "info": f"{ansi_esc.fg.GREEN}{{}}{ansi_esc.fg.RESET}",
    "warning": (
        f"{ansi_esc.fg.YELLOW}{ansi_esc.style.BRIGHT}" "{}" f"{ansi_esc.style.NORMAL}{ansi_esc.fg.RESET}"
    ),
    "error": (
        f"{ansi_esc.fg.RED}{ansi_esc.style.BRIGHT}" "{}" f"{ansi_esc.style.NORMAL}{ansi_esc.fg.RESET}"
    ),
    "critical": (
        f"{ansi_esc.fg.MAGENTA}{ansi_esc.style.BRIGHT}"
        "{}"
        f"{ansi_esc.fg.RESET}{ansi_esc.style.NORMAL}"
    ),
}


def log_level_caps(levelno: int, levelname: str) -> str:
    if levelno < logging.WARNING:
        return levelname.lower()
    return levelname


def log_level_color(levelno: int, base_levelname: str) -> str:
    if levelno < logging.INFO:
        return _COLOR_LEVEL_MAP["low"].format(base_levelname)
    elif levelno < logging.WARNING:
        return _COLOR_LEVEL_MAP["info"].format(base_levelname)
    elif levelno < logging.ERROR:
        return _COLOR_LEVEL_MAP["warning"].format(base_levelname)
    elif levelno < logging.CRITICAL:
        return _COLOR_LEVEL_MAP["error"].format(base_levelname)
    return _COLOR_LEVEL_MAP["critical"].format(base_levelname)


class ThdsLogfmter(Logfmter):
    @classmethod
    def get_extra(cls, record: logging.LogRecord) -> dict:
        """
        Return a dictionary of logger extra parameters by filtering any reserved keys.
        """
        extra = dict(super().get_extra(record))
        th_ctx = extra.pop(TH_REC_CTXT, None)
        if th_ctx:
            extra.update(th_ctx)
        return extra

    def format_key_value(self, record: logging.LogRecord, key: str, value) -> str:
        if key == "mod":
            return f"mod={ThdsCompactFormatter.format_module_name(value)}"
        if key == "lvl":
            core_str = log_level_color(record.levelno, f"lvl={log_level_caps(record.levelno, value)}")
            return core_str + " " * (7 - len(record.levelname))
        return super().format_key_value(record, key, value)


def mk_default_logfmter() -> ThdsLogfmter:
    return ThdsLogfmter(
        keys=["lvl", "mod"],
        mapping={"lvl": "levelname", "mod": "name"},
    )
