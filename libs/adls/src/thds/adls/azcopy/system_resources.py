import os

from thds.core import cache, cpus, log

logger = log.getLogger(__name__)


@cache.locking
def restrict_usage() -> dict:
    num_cpus = cpus.available_cpu_count()

    env = dict(os.environ)
    if "AZCOPY_BUFFER_GB" not in os.environ:
        likely_mem_gb_available = num_cpus * 4  # assume 4 GB per CPU core is available
        # o3 suggested 15% of the total available memory...
        env["AZCOPY_BUFFER_GB"] = str(likely_mem_gb_available * 0.15)
    if "AZCOPY_CONCURRENCY" not in os.environ:
        env["AZCOPY_CONCURRENCY"] = str(int(num_cpus * 2))

    logger.info(
        "AZCOPY_BUFFER_GB == %s and AZCOPY_CONCURRENCY == %s",
        env["AZCOPY_BUFFER_GB"],
        env["AZCOPY_CONCURRENCY"],
    )
    return env
