import functools
import time
from typing import Callable

from thds.core.log import getLogger

# author: matt.eby


def timer(func: Callable):
    """
    Decorator to add logging of timer information to a function invocation. Logs when entering a function and then logs
    with time information when exiting.
    :param func:
        Function to decorate with timing info.
    :return:
        Wrapped function.
    """
    logger = getLogger(func.__module__)

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = time.time()
        start_formatted = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(start))

        logger.info("Starting %r at %s", func.__name__, start_formatted)
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time

        end = time.time()
        end_formatted = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(end))
        logger.info("Finished %r in %0.4f secs at %s", func.__name__, run_time, end_formatted)
        return value

    return wrapper_timer
