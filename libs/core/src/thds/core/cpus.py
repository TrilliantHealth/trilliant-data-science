import multiprocessing
import os
import typing as ty
from pathlib import Path

from . import log

_CPU_QUOTA_PATH = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
_CPU_PERIOD_PATH = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
# standard linux kernel cgroup config files
# https://www.kernel.org/doc/html/latest/scheduler/sched-bwc.html#management
_CPU_MAX_PATH_V2 = Path("/sys/fs/cgroup/cpu.max")
# cgroups v2: https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#cgroup-v2-cpu
_CPU_SHARES_PATH = Path("/sys/fs/cgroup/cpu/cpu.shares")
# some kind of redhat thing: https://www.redhat.com/en/blog/cgroups-part-two

logger = log.getLogger(__name__)

T = ty.TypeVar("T")


def _try_read_value(config_path: Path, parse: ty.Callable[[str], T]) -> ty.Optional[T]:
    if config_path.is_file():
        logger.info(f"Found {config_path}; attempting to read value")
        with config_path.open() as f:
            contents = f.read().strip()
            try:
                value = parse(contents)
            except Exception as e:
                logger.error(f"Could not parse value from {contents} with {parse}: {e}")
                # if the file exists but we can't parse the contents, something is very wrong with our assumptions;
                # better to fail loudly than risk silent CPU oversubscription
                raise e
            else:
                logger.info(f"Read value {value} from {config_path}")
                return value
    return None


def _parse_cpu_quota_and_period_v2(s: str) -> ty.Tuple[int, int]:
    """Parse both CPU quota and period from kernel cgroup v2 config file."""

    parts = s.split()
    quota_str, period_str = parts[0], parts[1]

    if quota_str == "max":
        # https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
        #
        # "In the above four control files, the special token “max” should be used
        # to represent upward infinity for both reading and writing."
        quota = -1  # Use -1 to indicate unlimited, matching v1 behavior
    else:
        quota = int(quota_str)

    period = int(period_str)
    return quota, period


def available_cpu_count() -> int:
    """Attempt to determine number of available CPUs, accounting for the possibility of running inside a docker
    container. Ideally, this serves as a drop-in replacement for os.cpu_count() in that context.

    Partially cribbed from a suggestion in https://bugs.python.org/issue36054, and partially from joblib (specifically
    handling v2 of the kernel cgroup spec).
    """
    if hasattr(os, "sched_getaffinity"):
        cpu_count = len(os.sched_getaffinity(0))
    else:
        cpu_count = multiprocessing.cpu_count()

    cpu_quota_us: ty.Optional[int]
    cpu_period_us: ty.Optional[int]
    if (
        quota_and_period_v2 := _try_read_value(_CPU_MAX_PATH_V2, _parse_cpu_quota_and_period_v2)
    ) is not None:
        # this file contains both values
        cpu_quota_us, cpu_period_us = quota_and_period_v2
    else:
        cpu_quota_us = _try_read_value(_CPU_QUOTA_PATH, int)
        cpu_period_us = _try_read_value(_CPU_PERIOD_PATH, int)

    if cpu_quota_us is not None and cpu_period_us is not None and cpu_quota_us != -1:
        cpu_shares = int(cpu_quota_us / cpu_period_us)
    elif cpu_shares_ := _try_read_value(_CPU_SHARES_PATH, int):
        cpu_shares = int(cpu_shares_ / 1024)
    else:
        logger.info(f"Using naive CPU count: {cpu_count}")
        return cpu_count

    logger.info(
        f"Determined CPU shares from quota and period: {cpu_shares}; returning lesser of this and naive "
        f"CPU count: {cpu_count}"
    )
    return min(cpu_shares, cpu_count)


def ci_sensitive_cpu_count(default_num_workers_for_ci: int = 4) -> int:
    return default_num_workers_for_ci if "CI" in os.environ else available_cpu_count()
