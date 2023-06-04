import subprocess
from pathlib import Path

from thds.adls import download
from thds.core.log import getLogger
from thds.mops.remote import AdlsPickleRunner
from thds.mops.remote import adls_remote_files as arf
from thds.mops.remote import pure_remote

logger = getLogger(__name__)

# just set up a different global cache
arf._srcfile_cache = download.Cache(Path(__file__).parent / ".adls-md5-ro-cache", True)
# this is ugly but it also just doesn't really matter


def _subprocess_remote(args_list):
    logger.info(f"Invoking shell runner with args {args_list}")
    subprocess.run(args_list)
    logger.info("Completed shell runner")


runner = AdlsPickleRunner(_subprocess_remote)


def clear_cache():
    pass


adls_shell = pure_remote(runner)
