import logging
import time
import typing as ty
from timeit import default_timer

from thds.mops.colorize import colorized
from thds.mops.config import adls_max_clients
from thds.mops.remote import Thunk, parallel_yield_results

from ._util import adls_shell


def log_debug(handler: logging.Handler, logger: str = "urllib3"):
    std_formatter = logging.getLogger().handlers[0].formatter
    assert std_formatter
    handler.setFormatter(std_formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger(logger).setLevel(logging.DEBUG)


log_debug(logging.FileHandler("stress.log"))


BROWN = colorized(fg="brown", bg="black")


@adls_shell
def run_and_sleep(i: int, data: ty.List[float], sleep: int) -> float:
    the_sum = sum(data)
    print(BROWN(f"remote {i} - sum: {the_sum} - sleeping!"))
    time.sleep(sleep)
    return the_sum


def stress(max_clients: int = 10, n: int = 200, sleep: int = 5):
    """This will perform 5 ADLS operations (2 file exists, 1 push, one
    pull, then one final push per thread. The computation by
    definition takes N seconds, so that should give a rough idea of
    how many tasks we're executing in parallel over the period of time
    that the test is running.
    """
    start = default_timer()
    with adls_max_clients.set(max_clients):
        tasks = [Thunk(run_and_sleep, i, list(range(i * n, (i + 1) * n)), sleep) for i in range(n)]

        assert len(list(parallel_yield_results(tasks))) == n

    total = default_timer() - start
    print(f"With max_clients {max_clients}; n {n}; sleep {sleep}, took {int(total)} seconds")


# python -c "from tests.test_mops.integration.remote.stress_test import stress; stress(20, 100)"
