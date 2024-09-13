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

from .basic_config import DuplicateFilter, set_logger_to_console_level  # noqa: F401
from .kw_formatter import ThdsCompactFormatter  # noqa: F401
from .kw_logger import KwLogger, getLogger, logger_context, make_th_formatters_safe  # noqa: F401
