import subprocess

from thds.core.log import getLogger
from thds.mops.pure import MemoizingPicklingRunner, use_runner

from ...config import TEST_TMP_URI

logger = getLogger(__name__)


def _subprocess_remote(args_list):
    logger.info(f"Invoking shim runner with args {args_list}")
    subprocess.run(["python", "-m", "thds.mops.pure.core.entry.main", *args_list])
    logger.info("Completed shim runner")


runner = MemoizingPicklingRunner(_subprocess_remote, TEST_TMP_URI)


def clear_cache():
    pass


adls_shim = use_runner(runner)
