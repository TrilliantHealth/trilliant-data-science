#!/usr/bin/env python
import logging
import os

os.environ["THDS_CORE_LOG_LEVEL"] = "DEBUG"  # noqa


from thds.core import log  # noqa

logger = log.getLogger(__name__)


for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL):
    logger.log(level, f"this is a {level} message")
