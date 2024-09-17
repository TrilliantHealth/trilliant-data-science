"""This is the 'standard' keyword-formatting logger formatter for our logs.

It is enabled by default via basic_config.py, but is not required.
"""

import logging
import typing as ty

from .. import ansi_esc, config
from .kw_logger import th_keyvals_from_record

MAX_MODULE_NAME_LEN = config.item("max_module_name_len", 40, parse=int)
_MODULE_NAME_FMT_STR = "{compressed_name:" + str(MAX_MODULE_NAME_LEN()) + "}"

_COLOR_LEVEL_MAP = {
    "low": f"{ansi_esc.fg.BLUE}{{}}{ansi_esc.fg.RESET}",
    "info": f"{ansi_esc.fg.GREEN}{{}}{ansi_esc.fg.RESET}",
    "warning": (
        f"{ansi_esc.fg.YELLOW}{ansi_esc.style.BRIGHT}" "{}" f"{ansi_esc.style.NORMAL}{ansi_esc.fg.RESET}"
    ),
    "error": (
        f"{ansi_esc.bg.ERROR_RED}{ansi_esc.style.BRIGHT}"
        "{}"
        f"{ansi_esc.style.NORMAL}{ansi_esc.bg.RESET}"
    ),
    "critical": (
        f"{ansi_esc.bg.MAGENTA}{ansi_esc.style.BRIGHT}{ansi_esc.style.BLINK}"  # 😂
        "{}"
        f"{ansi_esc.bg.RESET}{ansi_esc.style.NORMAL}{ansi_esc.style.NO_BLINK}"
    ),
}


def log_level_color(levelno: int, base_levelname: str) -> str:
    if levelno < logging.INFO:
        return _COLOR_LEVEL_MAP["low"].format(base_levelname.lower())
    elif levelno < logging.WARNING:
        return _COLOR_LEVEL_MAP["info"].format(base_levelname.lower())
    elif levelno < logging.ERROR:
        return _COLOR_LEVEL_MAP["warning"].format(base_levelname)
    elif levelno < logging.CRITICAL:
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
        levelname = log_level_color(record.levelno, base_levelname)

        th_ctx: ty.Any = th_keyvals_from_record(record) or tuple()
        short_name = self.format_module_name(record.name)
        formatted = f"{self.formatTime(record)} {levelname}  {short_name} {th_ctx} {record.message}"
        if exc_text := self._format_exception_and_trace(record):
            formatted += exc_text
        return formatted
