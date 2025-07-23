import subprocess
import time
import typing as ty
from timeit import default_timer

from thds.adls import defaults
from thds.core.log import getLogger
from thds.mops.config import max_concurrent_network_ops
from thds.mops.parallel import Thunk, parallel_yield_results
from thds.mops.pure import MemoizingPicklingRunner, use_runner
from thds.termtool.colorize import colorized

BROWN = colorized(fg="brown", bg="black")

logger = getLogger(__name__)


def _subprocess_remote(args_list: ty.Sequence[str]) -> None:
    logger.info(f"Invoking 'remote' runner with args {args_list}")
    subprocess.run(args_list)
    logger.info("Completed 'remote' runner")


runner = MemoizingPicklingRunner(_subprocess_remote, defaults.mops_root)
adls_shim = use_runner(runner)


@adls_shim
def run_and_sleep(i: int, data: ty.List[float], sleep: int) -> float:
    """Runs 'remotely' - arguments are pickled and passed via ADLS; result is returned via ADLS."""
    the_sum = sum(data)
    print(BROWN(f"remote {i} - sum: {the_sum} - sleeping!"))
    time.sleep(sleep)
    return the_sum


def stress(max_clients: int, n: int, sleep: int) -> None:
    """MemoizingPicklingRunner will perform 4 local ADLS operations (1 file
    exists, 1 push, 1 file exists and 1 file pull) per task. The
    remote runner will perform 2 more ADLS operations, which in this
    case will also be occurring on the local machine, using a
    different client per runner. This gives a total of 6 ADLS
    operations for this test, whereas a properly remote worker would
    allow those 2 remote operations to be offloaded.

    The computation by definition takes N seconds, but can in theory
    be perfectly parallelized, so this gives some idea of how the
    overhead of launching and retrieving task results increases as the
    length of the task decreases relative to the number of total tasks.
    """
    start = default_timer()
    with max_concurrent_network_ops.set_local(max_clients):
        tasks = [Thunk(run_and_sleep, i, list(range(i * n, (i + 1) * n)), sleep) for i in range(n)]

        assert len(list(parallel_yield_results(tasks))) == n

    total = default_timer() - start
    print(
        f"With max_clients {max_clients}; n {n}; sleep {sleep}, took {total:.1f} seconds,"
        f" which is {total/n:.2f} seconds per task."
        " Prior experiments have found this to stabilize with increasing N in the vicinity of 0.2 seconds"
        " of overhead per task as long as the # of tasks dominates (>=20x) the length (in seconds) of the tasks."
    )
