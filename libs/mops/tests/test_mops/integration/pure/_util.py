import subprocess
from pathlib import Path

from thds.adls import download
from thds.core.log import getLogger
from thds.mops.pure import AdlsPickleRunner, use_runner
from thds.mops.pure.adls.srcdest import download as dl

logger = getLogger(__name__)

# just set up a different global cache
dl.srcfile_cache = download.Cache(Path(__file__).parent / ".adls-md5-ro-cache", True)
# this is ugly but it also just doesn't really matter


def _subprocess_remote(args_list):
    logger.info(f"Invoking shell runner with args {args_list}")
    subprocess.run(["python", "-m", "thds.mops.pure.core.entry.main", *args_list])
    logger.info("Completed shell runner")


runner = AdlsPickleRunner(_subprocess_remote, "adls://thdsscratch/tmp/")


def clear_cache():
    pass


adls_shell = use_runner(runner)
