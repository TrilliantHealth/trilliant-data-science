"""Contains the basic configuration for our logger. By importing thds.core.log, you import
and 'use' this configuration.
"""

import logging
import logging.config
import os
import sys
import typing as ty
from datetime import datetime
from pathlib import Path
from typing import Iterator, Tuple

from .. import config, home
from .json_formatter import ThdsJsonFormatter
from .kw_formatter import ThdsCompactFormatter
from .kw_logger import LOGLEVEL, getLogger, make_th_formatters_safe
from .logfmt import mk_default_logfmter

_LOG_FILEPATH = os.getenv(
    "THDS_CORE_LOG_FILEPATH",
    str(
        # we're logging to a file by default now. Set this to empty string to turn off.
        # It's not a config item because it can't usefully be set after startup.
        home.HOMEDIR()
        / ".thds-logs"
        / "-".join(
            [
                datetime.now().isoformat(),
                f"ppid_{os.getppid()}",
                f"pid_{os.getpid()}",
                f"{'_'.join(sys.argv)[:150]}.log",
            ]
        ).replace("/", "_")
    ),
)


_LOGLEVELS_FILEPATH = config.item("thds.core.log.levels_file", "", parse=lambda s: s.strip())
# see _parse_thds_loglevels_file for format of this file.

FORMAT = config.item("thds.core.log.format", "")  # valid options are 'logfmt', 'json', and ''.


def _pick_formatter() -> ty.Callable[[], logging.Formatter]:
    if FORMAT() == "logfmt":
        return mk_default_logfmter
    if FORMAT() == "json":
        return ThdsJsonFormatter
    return ThdsCompactFormatter


# this is the base of what gets passed to logging.dictConfig.
_BASE_LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"()": _pick_formatter()}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "default"}},
    "root": {"handlers": ["console"]},
}


def set_logger_to_console_level(config: dict, logger_name: str, level: int) -> dict:
    if logger_name == "*":
        if level != LOGLEVEL():
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
    current_level = LOGLEVEL()
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

    if _LOG_FILEPATH:
        log_path = Path(_LOG_FILEPATH)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            # ^ I hate doing IO in module scope, but we've waited until the last possible moment...
            live_config["handlers"]["file"] = {  # type: ignore
                "class": "logging.FileHandler",
                "formatter": "default",
                "filename": _LOG_FILEPATH,
                "delay": True,  # no need to have empty logfiles sitting around
            }
            live_config["root"]["handlers"].append("file")  # type: ignore
        except Exception as err:
            print(f"Unable to create log directory at '{log_path.parent}' - ERROR: {err}")

        orig_excepthook = sys.excepthook

        def log_exception_to_file(*exc_info):
            logger = logging.getLogger()
            console_handler = next(h for h in logger.handlers if isinstance(h, logging.StreamHandler))
            prev_level = console_handler.level
            console_handler.setLevel(logging.CRITICAL + 1)  # temporarily disable
            logger.error("logging uncaught exception to file", exc_info=exc_info)
            console_handler.setLevel(prev_level)  # restore
            orig_excepthook(*exc_info)

        sys.excepthook = log_exception_to_file

    logging.config.dictConfig(live_config)
    make_th_formatters_safe(logging.getLogger())

    class StartsWithFilter(logging.Filter):
        def __init__(self, startswith: str):
            self.startswith = startswith

        def filter(self, record):
            return not record.name.startswith(self.startswith)

    for noisy_logger in ("py4j.java_gateway", "py4j.clientserver"):  # 11.3, 9.1
        logging.getLogger(noisy_logger).addFilter(StartsWithFilter(noisy_logger))
