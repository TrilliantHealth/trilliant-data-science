import subprocess

from thds.core.log import getLogger
from thds.mops.remote import AdlsPickleRunner, pure_remote

logger = getLogger(__name__)


def _subprocess_remote(args_list):
    logger.info(f"Invoking shell runner with args {args_list}")
    subprocess.run(args_list)
    logger.info("Completed shell runner")


runner = AdlsPickleRunner(_subprocess_remote)


def clear_cache():
    runner._pre_run_file_exists.cache_clear()  # type: ignore


adls_shell = pure_remote(runner)
