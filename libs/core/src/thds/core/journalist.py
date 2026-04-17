"""Journalist: sits alongside your process tree, watches everything, and reports on it.

Samples RSS, CPU, and network IO across the full process tree at configurable
intervals.  Uses psutil when available; graceful no-op otherwise.  On Linux
containers, reads cgroup memory for accurate container-level RSS (avoids
psutil overcounting from COW-shared pages in forked processes).
"""

import os
import threading
import time
import typing as ty
from dataclasses import dataclass
from pathlib import Path

from thds.core import log

_logger = log.getLogger(__name__)

try:
    import psutil  # type: ignore[import-untyped]
except ImportError:
    psutil = None  # type: ignore[assignment]


_ACTIVE: "Journalist | None" = None
_ACTIVE_LOCK = threading.Lock()


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


def _read_net_bytes() -> tuple[int, int] | None:
    """Return (bytes_sent, bytes_recv) across all interfaces, or None."""
    if psutil is None:
        return None

    try:
        counters = psutil.net_io_counters()
        return counters.bytes_sent, counters.bytes_recv
    except Exception:
        return None


@dataclass(frozen=True)
class JournalistMetrics:
    """Snapshot of resource usage collected by a Journalist."""

    peak_rss_gb: float
    avg_rss_gb: float
    peak_cpu_cores: float
    avg_cpu_cores: float
    peak_recv_mbps: float
    total_recv_gb: float
    peak_sent_mbps: float
    total_sent_gb: float


class Journalist:
    """Context manager that samples RSS, CPU, and network IO across the process tree.

    Logs memory, CPU cores, and network bandwidth at each interval.
    No-op if psutil is unavailable.
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
        # Network IO tracking.
        self._net_available = _read_net_bytes() is not None
        self._start_net_recv = 0
        self._start_net_sent = 0
        self._peak_recv_mbps = 0.0
        self._peak_sent_mbps = 0.0

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

        # "Log window" baselines - reset at each log emission so "current" values
        # report the average rate (or window peak for mem) over the window
        # instead of the last sample.
        window_start_wall = prev_wall
        window_start_cpu = prev_cpu
        window_peak_rss_mb = 0.0
        window_peak_cgroup_mb = 0.0

        prev_net = _read_net_bytes()
        if prev_net is not None:
            self._start_net_sent, self._start_net_recv = prev_net
        prev_net_recv = self._start_net_recv
        prev_net_sent = self._start_net_sent
        window_start_net_recv = self._start_net_recv
        window_start_net_sent = self._start_net_sent

        _BLUE = "\033[34m"
        _GREEN = "\033[32m"
        _YELLOW = "\033[33m"
        _RESET = "\033[0m"

        while not self._stop.wait(self._sample_interval):
            rss_mb = _tree_rss_bytes(proc) / (1024 * 1024)
            self._peak_rss_mb = max(self._peak_rss_mb, rss_mb)
            self._rss_sum_mb += rss_mb
            self._sample_count += 1
            window_peak_rss_mb = max(window_peak_rss_mb, rss_mb)

            if self._cgroup_available:
                cg_bytes = _read_cgroup_mem_bytes()
                if cg_bytes is not None:
                    cgroup_mb = cg_bytes / (1024 * 1024)
                    self._peak_cgroup_mb = max(self._peak_cgroup_mb, cgroup_mb)
                    self._cgroup_sum_mb += cgroup_mb
                    window_peak_cgroup_mb = max(window_peak_cgroup_mb, cgroup_mb)

            now_wall = time.monotonic()
            cur_cpu = self._read_cpu_seconds(proc)

            dt = now_wall - prev_wall
            cpu_delta = max(0.0, cur_cpu - prev_cpu)
            sample_cores = cpu_delta / max(dt, 0.01)
            self._peak_cpu_cores = max(self._peak_cpu_cores, sample_cores)

            if self._net_available:
                cur_net = _read_net_bytes()
                if cur_net is not None:
                    sent_delta = max(0, cur_net[0] - prev_net_sent)
                    recv_delta = max(0, cur_net[1] - prev_net_recv)
                    sample_sent_mbps = (sent_delta / max(dt, 0.01)) / (1024 * 1024)
                    sample_recv_mbps = (recv_delta / max(dt, 0.01)) / (1024 * 1024)
                    self._peak_sent_mbps = max(self._peak_sent_mbps, sample_sent_mbps)
                    self._peak_recv_mbps = max(self._peak_recv_mbps, sample_recv_mbps)
                    prev_net_sent = cur_net[0]
                    prev_net_recv = cur_net[1]

            prev_wall, prev_cpu = now_wall, cur_cpu
            self._last_wall, self._last_cpu = now_wall, cur_cpu

            if now_wall - window_start_wall >= self._log_interval:
                total_wall = now_wall - self._start_wall
                total_cpu = max(0.0, cur_cpu - self._start_cpu)
                avg_cores = total_cpu / max(total_wall, 0.01)

                window_seconds = max(now_wall - window_start_wall, 0.01)
                window_cores = max(0.0, cur_cpu - window_start_cpu) / window_seconds

                if self._cgroup_available:
                    window_peak_mem_gb = window_peak_cgroup_mb / 1024
                    peak_mem_gb = self._peak_cgroup_mb / 1024
                else:
                    window_peak_mem_gb = window_peak_rss_mb / 1024
                    peak_mem_gb = self._peak_rss_mb / 1024

                elapsed_min = total_wall / 60

                total_recv_gb = (prev_net_recv - self._start_net_recv) / (1024**3)
                total_sent_gb = (prev_net_sent - self._start_net_sent) / (1024**3)

                net_part = ""
                if self._net_available:
                    window_recv_mbps = (
                        (prev_net_recv - window_start_net_recv) / window_seconds / (1024 * 1024)
                    )
                    window_sent_mbps = (
                        (prev_net_sent - window_start_net_sent) / window_seconds / (1024 * 1024)
                    )
                    net_part = (
                        f" | {_YELLOW}NET {window_recv_mbps:.0f}/{self._peak_recv_mbps:.0f}\u2193"
                        f" {window_sent_mbps:.0f}/{self._peak_sent_mbps:.0f}\u2191 cur/peak MBps;"
                        f" total GB {total_recv_gb:.1f}\u2193 {total_sent_gb:.1f}\u2191{_RESET}"
                    )

                _logger.info(
                    "%s [%.1fm]: %sMEM cur %.1f / %.1f peak GB%s"
                    " | %sCPU cur %.1f, avg %.1f / %.1f peak%s%s",
                    self._label,
                    elapsed_min,
                    _BLUE,
                    window_peak_mem_gb,
                    peak_mem_gb,
                    _RESET,
                    _GREEN,
                    window_cores,
                    avg_cores,
                    self._peak_cpu_cores,
                    _RESET,
                    net_part,
                )

                window_start_wall = now_wall
                window_start_cpu = cur_cpu
                window_start_net_recv = prev_net_recv
                window_start_net_sent = prev_net_sent
                window_peak_rss_mb = 0.0
                window_peak_cgroup_mb = 0.0

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

    @property
    def peak_recv_mbps(self) -> float:
        return self._peak_recv_mbps

    @property
    def peak_sent_mbps(self) -> float:
        return self._peak_sent_mbps

    @property
    def total_recv_gb(self) -> float:
        net = _read_net_bytes()
        if net is None:
            return 0.0

        return max(0, net[1] - self._start_net_recv) / (1024**3)

    @property
    def total_sent_gb(self) -> float:
        net = _read_net_bytes()
        if net is None:
            return 0.0

        return max(0, net[0] - self._start_net_sent) / (1024**3)

    @property
    def metrics(self) -> JournalistMetrics:
        # Prefer cgroup when available; psutil's proc-tree RSS double-counts
        # COW-shared pages across forked children (observed: 417 GB peak on a
        # 256 GB node). Cgroup is the kernel's authoritative container view.
        peak_mem_mb = self._peak_cgroup_mb if self._cgroup_available else self._peak_rss_mb
        avg_mem_mb = self.avg_cgroup_mb if self._cgroup_available else self.avg_rss_mb
        return JournalistMetrics(
            peak_rss_gb=peak_mem_mb / 1024,
            avg_rss_gb=avg_mem_mb / 1024,
            peak_cpu_cores=self.peak_cpu_cores,
            avg_cpu_cores=self.avg_cpu_cores,
            peak_recv_mbps=self.peak_recv_mbps,
            total_recv_gb=self.total_recv_gb,
            peak_sent_mbps=self.peak_sent_mbps,
            total_sent_gb=self.total_sent_gb,
        )

    def __enter__(self) -> "Journalist":
        global _ACTIVE
        if not self._enabled:
            return self

        with _ACTIVE_LOCK:
            if _ACTIVE is not None:
                _logger.debug(
                    "Journalist already active in this process; %r will be a no-op.", self._label
                )
                self._enabled = False
                return self

            _ACTIVE = self

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _log_final_summary(self) -> None:
        # Parens format matches the periodic line's "avg/peak" grammar; "cur"
        # is dropped because it is meaningless at exit. For NET we lead with
        # peak (sustained throughput is captured in total GB, so a recap avg
        # rate adds nothing).
        m = self.metrics
        elapsed_min = (self._last_wall - self._start_wall) / 60
        _BLUE = "\033[34m"
        _GREEN = "\033[32m"
        _YELLOW = "\033[33m"
        _RESET = "\033[0m"
        net_part = ""
        if self._net_available:
            net_part = (
                f" | {_YELLOW}NET {m.peak_recv_mbps:.0f}\u2193 {m.peak_sent_mbps:.0f}\u2191 peak MBps;"
                f" total GB {m.total_recv_gb:.1f}\u2193 {m.total_sent_gb:.1f}\u2191{_RESET}"
            )
        _logger.info(
            "%s [%.1fm]: %sMEM avg %.1f / %.1f peak GB%s | %sCPU avg %.1f / %.1f peak%s%s",
            self._label,
            elapsed_min,
            _BLUE,
            m.avg_rss_gb,
            m.peak_rss_gb,
            _RESET,
            _GREEN,
            m.avg_cpu_cores,
            m.peak_cpu_cores,
            _RESET,
            net_part,
        )

    def __exit__(self, *exc: ty.Any) -> None:
        global _ACTIVE
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=2.0)
            if self._sample_count > 0:
                self._log_final_summary()

        with _ACTIVE_LOCK:
            if _ACTIVE is self:
                _ACTIVE = None
