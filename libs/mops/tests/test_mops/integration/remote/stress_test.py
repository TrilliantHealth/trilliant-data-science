import time
import typing as ty

from thds.mops.config import adls_max_clients
from thds.mops.remote import Thunk, parallel_yield_results

from ._util import adls_shell


@adls_shell
def run_and_sleep(i: int, data: ty.List[float], sleep: int) -> float:
    the_sum = sum(data)
    print(f"remote {i} - sum: {the_sum} - sleeping!")
    time.sleep(sleep)
    return the_sum


def stress(p: int):
    """This will perform 5 ADLS operations (2 file exists, 1 push, one
    pull, then one final push per thread. The computation by
    definition takes N seconds, so that should give a rough idea of
    how many tasks we're executing in parallel over the period of time
    that the test is running.
    """
    N = 200
    with adls_max_clients.set(p):
        SLEEP = 5
        tasks = [Thunk(run_and_sleep, i, list(range(i * N, (i + 1) * N)), SLEEP) for i in range(N)]

        assert len(list(parallel_yield_results(tasks))) == N


# python -c "from tests.integration.remote.stress_test import stress; stress(30)"
