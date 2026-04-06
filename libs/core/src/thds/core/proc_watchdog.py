"""Process-tree resource watchdog: samples RSS and CPU across the full process tree.

Uses psutil when available; graceful no-op otherwise.  On Linux containers,
also reads cgroup memory for accurate container-level RSS (avoids psutil
overcounting from COW-shared pages in forked processes).
"""

import os
import threading
import time
import typing as ty
from pathlib import Path

from thds.core import log

_logger = log.getLogger(__name__)

try:
    import psutil  # type: ignore[import-untyped]
except ImportError:
    psutil = None  # type: ignore[assignment]


# cgroup v2: /sys/fs/cgroup/memory.current
# cgroup v1: /sys/fs/cgroup/memory/memory.usage_in_bytes
_CGROUP_V2 = Path("/sys/fs/cgroup/memory.current")
_CGROUP_V1 = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")

# cgroup v2: /sys/fs/cgroup/cpu.stat (contains usage_usec)
# cgroup v1: /sys/fs/cgroup/cpu/cpuacct.usage (nanoseconds)
_CGROUP_CPU_V2 = Path("/sys/fs/cgroup/cpu.stat")
_CGROUP_CPU_V1 = Path("/sys/fs/cgroup/cpu/cpuacct.usage")


def _read_cgroup_mem_bytes() -> int | None:
    """Read container memory from cgroup, or None if unavailable."""
    for p in (_CGROUP_V2, _CGROUP_V1):
        try:
            return int(p.read_text().strip())
        except (FileNotFoundError, ValueError, PermissionError):
            continue
    return None


def _read_cgroup_cpu_seconds() -> float | None:
    """Read cumulative container CPU seconds from cgroup, or None if unavailable."""
    try:
        text = _CGROUP_CPU_V2.read_text()
        for line in text.splitlines():
            if line.startswith("usage_usec"):
                return int(line.split()[1]) / 1_000_000
    except (FileNotFoundError, ValueError, PermissionError):
        pass

    try:
        return int(_CGROUP_CPU_V1.read_text().strip()) / 1_000_000_000
    except (FileNotFoundError, ValueError, PermissionError):
        pass

    return None


def _tree_rss_bytes(proc: "psutil.Process") -> int:
    try:
        total = proc.memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0

    for child in proc.children(recursive=True):
        try:
            total += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total


def _tree_cpu_times(proc: "psutil.Process") -> tuple[float, float]:
    """Return (user, system) CPU seconds summed across process tree."""
    try:
        t = proc.cpu_times()
        user, sys_ = t.user, t.system
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0.0, 0.0

    for child in proc.children(recursive=True):
        try:
            ct = child.cpu_times()
            user += ct.user
            sys_ += ct.system
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return user, sys_


class ProcessTreeWatchdog:
    """Context manager that samples RSS and CPU across the full process tree.

    Logs tree_rss, peak_rss, instantaneous CPU cores, peak CPU cores, and
    avg CPU cores each interval. No-op if psutil is unavailable.
    """

    def __init__(self, label: str, interval: float = 10.0, sample_interval: float = 1.0) -> None:
        self._label = label
        self._log_interval = interval
        self._sample_interval = sample_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._peak_rss_mb = 0.0
        self._peak_cpu_cores = 0.0
        self._rss_sum_mb = 0.0
        self._sample_count = 0
        self._enabled = psutil is not None
        # cgroup memory (accurate container-level, no fork overcounting).
        self._peak_cgroup_mb = 0.0
        self._cgroup_sum_mb = 0.0
        self._cgroup_available = _read_cgroup_mem_bytes() is not None
        self._cgroup_cpu_available = _read_cgroup_cpu_seconds() is not None
        # Set on __enter__, used for cumulative avg CPU.
        self._start_wall = 0.0
        self._start_cpu = 0.0
        self._last_wall = 0.0
        self._last_cpu = 0.0

    def _read_cpu_seconds(self, proc: "psutil.Process") -> float:
        """Read CPU seconds from cgroup (preferred) or process tree (fallback)."""
        if self._cgroup_cpu_available:
            val = _read_cgroup_cpu_seconds()
            if val is not None:
                return val

        user, sys_ = _tree_cpu_times(proc)
        return user + sys_

    def _run(self) -> None:
        proc = psutil.Process(os.getpid())
        prev_wall = time.monotonic()
        prev_cpu = self._read_cpu_seconds(proc)

        self._start_wall = prev_wall
        self._start_cpu = prev_cpu
        last_log_wall = prev_wall
        last_cores = 0.0
        last_cgroup_mb = 0.0
        last_rss_mb = 0.0

        _BLUE = "\033[34m"
        _GREEN = "\033[32m"
        _RESET = "\033[0m"

        while not self._stop.wait(self._sample_interval):
            rss_mb = _tree_rss_bytes(proc) / (1024 * 1024)
            self._peak_rss_mb = max(self._peak_rss_mb, rss_mb)
            self._rss_sum_mb += rss_mb
            self._sample_count += 1
            last_rss_mb = rss_mb

            cgroup_mb = 0.0
            if self._cgroup_available:
                cg_bytes = _read_cgroup_mem_bytes()
                if cg_bytes is not None:
                    cgroup_mb = cg_bytes / (1024 * 1024)
                    self._peak_cgroup_mb = max(self._peak_cgroup_mb, cgroup_mb)
                    self._cgroup_sum_mb += cgroup_mb
                    last_cgroup_mb = cgroup_mb

            now_wall = time.monotonic()
            cur_cpu = self._read_cpu_seconds(proc)

            dt = now_wall - prev_wall
            cpu_delta = max(0.0, cur_cpu - prev_cpu)
            cores = cpu_delta / max(dt, 0.01)
            self._peak_cpu_cores = max(self._peak_cpu_cores, cores)
            last_cores = cores

            prev_wall, prev_cpu = now_wall, cur_cpu
            self._last_wall, self._last_cpu = now_wall, cur_cpu

            if now_wall - last_log_wall >= self._log_interval:
                last_log_wall = now_wall
                total_wall = now_wall - self._start_wall
                total_cpu = max(0.0, cur_cpu - self._start_cpu)
                avg_cores = total_cpu / max(total_wall, 0.01)

                if self._cgroup_available:
                    mem_gb = last_cgroup_mb / 1024
                    peak_mem_gb = self._peak_cgroup_mb / 1024
                    avg_mem_gb = (self._cgroup_sum_mb / self._sample_count) / 1024
                else:
                    mem_gb = last_rss_mb / 1024
                    peak_mem_gb = self._peak_rss_mb / 1024
                    avg_mem_gb = (self._rss_sum_mb / self._sample_count) / 1024

                elapsed_min = total_wall / 60
                _logger.info(
                    "%s [%.1fm]: %smem %.1f / %.1f GB (avg %.1f)%s | %scpu %.1f / %.1f cores (avg %.1f)%s",
                    self._label,
                    elapsed_min,
                    _BLUE,
                    mem_gb,
                    peak_mem_gb,
                    avg_mem_gb,
                    _RESET,
                    _GREEN,
                    last_cores,
                    self._peak_cpu_cores,
                    avg_cores,
                    _RESET,
                )

    @property
    def peak_rss_mb(self) -> float:
        return self._peak_rss_mb

    @property
    def avg_rss_mb(self) -> float:
        return self._rss_sum_mb / max(self._sample_count, 1)

    @property
    def peak_cgroup_mb(self) -> float:
        return self._peak_cgroup_mb

    @property
    def avg_cgroup_mb(self) -> float:
        return self._cgroup_sum_mb / max(self._sample_count, 1)

    @property
    def peak_cpu_cores(self) -> float:
        return self._peak_cpu_cores

    @property
    def avg_cpu_cores(self) -> float:
        total_wall = self._last_wall - self._start_wall
        total_cpu = max(0.0, self._last_cpu - self._start_cpu)
        return total_cpu / max(total_wall, 0.01)

    def __enter__(self) -> "ProcessTreeWatchdog":
        if self._enabled:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc: ty.Any) -> None:
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=2.0)
