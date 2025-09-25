"""This module exists solely to preferentially use pysqlite3 over the
built-in sqlite where it is already available.

Our primary purpose for doing this is to enable larger amounts of
mem-mapped shared memory between processes consuming reference-data.

However, while pre-compiled binaries for pysqlite3 are available via
PyPi, they are only available for manylinux, and most of our
development is done on Macs.

Since this customization is mostly useful for strongly parallel usage
of reference-data, and since that does not often happen on Mac
laptops, this shim will allow us to defer doing extra work to get
compiled pysqlite3 on Macs for the time being.
"""

import os
from logging import getLogger

_DISABLE_PYSQLITE3 = bool(os.environ.get("REF_D_DISABLE_PYSQLITE3", False))

logger = getLogger(__name__)

# All of the following log statements include the string `pysqlite3` for searchability.
if not _DISABLE_PYSQLITE3:
    try:
        import pysqlite3 as sqlite3

        logger.info(f"Using pysqlite3 with SQLite version {sqlite3.sqlite_version}")
    except ImportError:
        logger.debug("Using sqlite3 module because pysqlite3 was not available")
        # this is DEBUG because it's the 'base case' for local dev and
        # there's no need to make logs for development use cases more
        # verbose. In production, one of the other two INFO logs is
        # likely to fire, and if not, this case can be inferred from
        # the lack of log.
        import sqlite3  # type: ignore
else:
    import sqlite3  # type: ignore

    logger.info("Using sqlite3 module because pysqlite3 was disabled via environment variable")
