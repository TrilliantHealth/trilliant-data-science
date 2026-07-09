"""Journalist: sits alongside your process tree, watches everything, and reports on it.

Samples RSS, CPU, and network IO across the full process tree at configurable
intervals.  Uses psutil when available; graceful no-op otherwise.  On Linux
containers, reads cgroup memory for accurate container-level RSS (avoids
psutil overcounting from COW-shared pages in forked processes).

Journalists nest: opening a second Journalist while one is already active does
not stop the first.  Both report independently - each with its own label, its
own aggregate statistics, and its own log line - while a single process-global
sampler thread does the underlying reads once per tick and folds each sample
into every active Journalist.
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


_SAMPLER_THREAD_NAME = "thds-core-journalist-sampler"


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


def _read_disk_bytes() -> tuple[int, int] | None:
    """Return (bytes_read, bytes_written) across all disks, or None.

    System-wide (like net counters), not per-process - captures DuckDB spill
    and partitioned-write IO to local pod disk that network counters miss.
    """
    if psutil is None:
        return None

    try:
        counters = psutil.disk_io_counters()
        return counters.read_bytes, counters.write_bytes
    except Exception:
        return None


@dataclass(frozen=True)
class JournalistMetrics:
    """Snapshot of resource usage collected by a Journalist.

    Every field defaults, so `JournalistMetrics()` is a valid zero instance and
    adding a new metric never breaks downstream code that constructs one (e.g. a
    baseline/zero fixture). The Journalist itself always fills every field.
    """

    peak_rss_gb: float = 0.0
    avg_rss_gb: float = 0.0
    peak_cpu_cores: float = 0.0
    avg_cpu_cores: float = 0.0
    peak_recv_mbps: float = 0.0
    total_recv_gb: float = 0.0
    peak_sent_mbps: float = 0.0
    total_sent_gb: float = 0.0
    peak_disk_read_mbps: float = 0.0
    total_disk_read_gb: float = 0.0
    peak_disk_write_mbps: float = 0.0
    total_disk_write_gb: float = 0.0
    # Optional with a None default so previously-pickled instances
    # deserialize without error.
    elapsed_seconds: ty.Optional[float] = None


@dataclass(frozen=True)
class _Sample:
    """One tick's raw readings, produced by the sampler, folded by accumulators.

    The sampler reads every global source once per tick and packages it here; a
    Journalist interprets these into its own peaks/sums/window without re-reading.
    """

    wall: float
    cpu_seconds: float
    rss_mb: float
    cgroup_mb: float | None
    net: tuple[int, int] | None  # (bytes_sent, bytes_recv)
    disk: tuple[int, int] | None  # (bytes_read, bytes_written)


class _Gauge:
    """Accumulator for an instantaneous measurement (e.g. RSS): peak + running mean.

    `observe` folds each sample's value in; `peak`/`avg` are all-window, and
    `window_peak` is the max since the last `reset_window` (for the periodic line).
    Callers decide which samples count - the baseline sample is not observed.
    """

    def __init__(self) -> None:
        self.peak = 0.0
        self.window_peak = 0.0
        self._sum = 0.0
        self._count = 0

    def observe(self, value: float) -> None:
        self.peak = max(self.peak, value)
        self.window_peak = max(self.window_peak, value)
        self._sum += value
        self._count += 1

    @property
    def avg(self) -> float:
        return self._sum / max(self._count, 1)

    @property
    def seen(self) -> bool:
        return self._count > 0 or self.peak > 0.0

    def reset_window(self) -> None:
        self.window_peak = 0.0


class _Counter:
    """Accumulator for a monotonic cumulative source (CPU seconds, net/disk bytes).

    Tracks the peak per-interval rate, the total since `start`, and the mean rate
    over any window. Unit-agnostic: the caller scales the raw delta (bytes->MBps,
    cpu-seconds->cores) after reading `peak_rate` / `total` / `window_rate`.
    """

    def __init__(self) -> None:
        self.peak_rate = 0.0
        self._start = 0.0
        self._last = 0.0
        self._window_start = 0.0

    def start(self, value: float) -> None:
        self._start = self._last = self._window_start = value

    def advance(self, value: float, dt: float) -> None:
        self.peak_rate = max(self.peak_rate, max(0.0, value - self._last) / max(dt, 0.01))
        self._last = value

    @property
    def total(self) -> float:
        return max(0.0, self._last - self._start)

    def avg_rate(self, elapsed: float) -> float:
        return self.total / max(elapsed, 0.01)

    def window_rate(self, window_seconds: float) -> float:
        return max(0.0, self._last - self._window_start) / max(window_seconds, 0.01)

    def reset_window(self) -> None:
        self._window_start = self._last


_BLUE = "\033[34m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_MAGENTA = "\033[35m"
_RESET = "\033[0m"

_MB = 1024 * 1024
_GB = 1024**3


class _Sampler:
    """Process-global owner of the single sampler thread.

    Reads every global source (proc-tree RSS, cgroup mem/cpu, net, disk) once per
    tick and folds the resulting `_Sample` into every registered Journalist, so N
    nested Journalists share one thread and one set of reads. Ticks at the finest
    `sample_interval` any active Journalist requested; the interval is re-read at
    the top of each loop, so a newly-registered shorter interval takes effect only
    after the current sleep finishes (the sampler is not woken mid-sleep).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._accumulators: list["Journalist"] = []
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._sample_interval = 1.0
        self._cgroup_available = _read_cgroup_mem_bytes() is not None
        self._cgroup_cpu_available = _read_cgroup_cpu_seconds() is not None
        self._net_available = _read_net_bytes() is not None
        self._disk_available = _read_disk_bytes() is not None

    def active_labels(self) -> set[str]:
        with self._lock:
            return {acc._label for acc in self._accumulators}

    def current_interval(self) -> float:
        return self._sample_interval

    def register(self, acc: "Journalist") -> None:
        with self._lock:
            acc._label = self._resolve_label(acc._label)
            first = not self._accumulators
            self._accumulators.append(acc)
            self._sample_interval = min(a._sample_interval for a in self._accumulators)
            if first:
                self._stop = threading.Event()
                self._thread = threading.Thread(target=self._run, name=_SAMPLER_THREAD_NAME, daemon=True)
                self._thread.start()

    def deregister(self, acc: "Journalist") -> None:
        with self._lock:
            if acc in self._accumulators:
                self._accumulators.remove(acc)
            empty = not self._accumulators
            thread = self._thread
            if empty:
                self._thread = None
            else:
                self._sample_interval = min(a._sample_interval for a in self._accumulators)

        if empty and thread is not None:
            self._stop.set()
            thread.join(timeout=2.0)

    def _resolve_label(self, label: str) -> str:
        # Caller holds self._lock, so the active-label check and the subsequent
        # append are atomic. Only concurrently-active labels collide.
        active = {acc._label for acc in self._accumulators}
        if label not in active:
            return label

        n = 2
        while f"{label}#{n}" in active:
            n += 1
        resolved = f"{label}#{n}"
        _logger.warning("Journalist label %r already active; reporting as %r instead.", label, resolved)
        return resolved

    def _read_cpu_seconds(self, proc: "psutil.Process") -> float:
        if self._cgroup_cpu_available:
            val = _read_cgroup_cpu_seconds()
            if val is not None:
                return val

        user, sys_ = _tree_cpu_times(proc)
        return user + sys_

    def _run(self) -> None:
        proc = psutil.Process(os.getpid())
        while not self._stop.wait(self._sample_interval):
            cgroup_mb: float | None = None
            if self._cgroup_available:
                cg_bytes = _read_cgroup_mem_bytes()
                if cg_bytes is not None:
                    cgroup_mb = cg_bytes / _MB

            sample = _Sample(
                wall=time.monotonic(),
                cpu_seconds=self._read_cpu_seconds(proc),
                rss_mb=_tree_rss_bytes(proc) / _MB,
                cgroup_mb=cgroup_mb,
                net=_read_net_bytes() if self._net_available else None,
                disk=_read_disk_bytes() if self._disk_available else None,
            )

            # Snapshot under the lock, fold outside it. A journalist that exits
            # mid-batch is still in this snapshot, but its fold self-vetoes via
            # the one-way `_active` flag it clears in __exit__ before logging its
            # final summary - so no periodic line or metrics mutation can race
            # past __exit__ without any per-fold locking.
            with self._lock:
                snapshot = list(self._accumulators)
            for acc in snapshot:
                acc.fold(sample)


_SAMPLER = _Sampler()


class Journalist:
    """Context manager that samples RSS, CPU, and network IO across the process tree.

    Logs memory, CPU cores, and network bandwidth at each interval.  No-op if
    psutil is unavailable.  Journalists nest: entering a second one while another
    is active reports both independently rather than suppressing the newcomer.
    """

    def __init__(self, label: str, interval: float = 10.0, sample_interval: float = 1.0) -> None:
        self._label = label
        self._log_interval = interval
        self._sample_interval = sample_interval
        self._enabled = psutil is not None
        # One-way flag cleared in __exit__ before the final summary. fold() checks
        # it and bails, so a fold from a stale sampler snapshot cannot emit a
        # periodic line or mutate metrics after this journalist has exited - no
        # per-fold lock needed. Monotonic single-writer bool, GIL-atomic to read.
        self._active = True
        # Instantaneous gauges (peak + mean) and monotonic counters (rate + total).
        # Each owns its own peak/sum/start/window state; fold feeds them slices of
        # the sample, so the accounting isn't spread across dozens of loose ints.
        self._rss = _Gauge()
        self._cgroup = _Gauge()
        self._cpu = _Counter()
        self._sent = _Counter()
        self._recv = _Counter()
        self._disk_read = _Counter()
        self._disk_write = _Counter()
        self._sample_count = 0
        # Wall-clock baselines, captured on the first fold so everything scopes to
        # this Journalist's own enter->exit window (not the whole process).
        self._started = False
        self._start_wall = 0.0
        self._prev_wall = 0.0
        self._last_wall = 0.0
        self._window_start_wall = 0.0

    def fold(self, sample: _Sample) -> None:
        """Fold one sample into this Journalist's accumulators; may emit a log line.

        The sampler calls this once per tick for every active Journalist. The
        first fold captures per-window baselines; every fold updates the gauges
        and counters and (when the log interval elapses) emits the labeled line.
        """
        if not self._active:
            return

        if not self._started:
            self._start(sample)
            return

        dt = sample.wall - self._prev_wall
        self._sample_count += 1

        self._rss.observe(sample.rss_mb)
        if sample.cgroup_mb is not None:
            self._cgroup.observe(sample.cgroup_mb)

        self._cpu.advance(sample.cpu_seconds, dt)
        if sample.net is not None:
            self._sent.advance(sample.net[0], dt)
            self._recv.advance(sample.net[1], dt)

        if sample.disk is not None:
            self._disk_read.advance(sample.disk[0], dt)
            self._disk_write.advance(sample.disk[1], dt)

        self._prev_wall = self._last_wall = sample.wall

        if sample.wall - self._window_start_wall >= self._log_interval:
            self._log_window(sample)

    def _start(self, sample: _Sample) -> None:
        """Capture baselines from the first sample this Journalist sees."""
        self._started = True
        self._start_wall = self._prev_wall = self._last_wall = self._window_start_wall = sample.wall
        self._cpu.start(sample.cpu_seconds)
        if sample.net is not None:
            self._sent.start(sample.net[0])
            self._recv.start(sample.net[1])

        if sample.disk is not None:
            self._disk_read.start(sample.disk[0])
            self._disk_write.start(sample.disk[1])

    def _mem(self) -> _Gauge:
        # Prefer cgroup when available; psutil's proc-tree RSS double-counts
        # COW-shared pages across forked children (observed: 417 GB peak on a
        # 256 GB node). Cgroup is the kernel's authoritative container view.
        return self._cgroup if self._cgroup.seen else self._rss

    def _log_window(self, sample: _Sample) -> None:
        total_wall = sample.wall - self._start_wall
        window_seconds = sample.wall - self._window_start_wall
        mem = self._mem()

        net_part = ""
        if sample.net is not None:
            net_part = (
                f" | {_YELLOW}NET {self._recv.window_rate(window_seconds) / _MB:.0f}/{self._recv.peak_rate / _MB:.0f}\u2193"
                f" {self._sent.window_rate(window_seconds) / _MB:.0f}/{self._sent.peak_rate / _MB:.0f}\u2191 cur/peak MBps;"
                f" total GB {self._recv.total / _GB:.1f}\u2193 {self._sent.total / _GB:.1f}\u2191{_RESET}"
            )

        disk_part = ""
        if sample.disk is not None:
            disk_part = (
                f" | {_MAGENTA}DISK {self._disk_read.window_rate(window_seconds) / _MB:.0f}/{self._disk_read.peak_rate / _MB:.0f}r"
                f" {self._disk_write.window_rate(window_seconds) / _MB:.0f}/{self._disk_write.peak_rate / _MB:.0f}w cur/peak MBps;"
                f" total GB {self._disk_read.total / _GB:.1f}r {self._disk_write.total / _GB:.1f}w{_RESET}"
            )

        _logger.info(
            "%s [%.1fm]: %sMEM cur %.1f / %.1f peak GB%s | %sCPU cur %.1f, avg %.1f / %.1f peak%s%s%s",
            self._label,
            total_wall / 60,
            _BLUE,
            mem.window_peak / 1024,
            mem.peak / 1024,
            _RESET,
            _GREEN,
            self._cpu.window_rate(window_seconds),
            self._cpu.avg_rate(total_wall),
            self._cpu.peak_rate,
            _RESET,
            net_part,
            disk_part,
        )

        self._window_start_wall = sample.wall
        for acc in (
            self._rss,
            self._cgroup,
            self._cpu,
            self._sent,
            self._recv,
            self._disk_read,
            self._disk_write,
        ):
            acc.reset_window()

    @property
    def peak_rss_mb(self) -> float:
        return self._rss.peak

    @property
    def avg_rss_mb(self) -> float:
        return self._rss.avg

    @property
    def peak_cgroup_mb(self) -> float:
        return self._cgroup.peak

    @property
    def avg_cgroup_mb(self) -> float:
        return self._cgroup.avg

    @property
    def peak_cpu_cores(self) -> float:
        return self._cpu.peak_rate

    @property
    def avg_cpu_cores(self) -> float:
        return self._cpu.avg_rate(self._last_wall - self._start_wall)

    @property
    def peak_recv_mbps(self) -> float:
        return self._recv.peak_rate / _MB

    @property
    def peak_sent_mbps(self) -> float:
        return self._sent.peak_rate / _MB

    @property
    def total_recv_gb(self) -> float:
        return self._recv.total / _GB

    @property
    def total_sent_gb(self) -> float:
        return self._sent.total / _GB

    @property
    def peak_disk_read_mbps(self) -> float:
        return self._disk_read.peak_rate / _MB

    @property
    def peak_disk_write_mbps(self) -> float:
        return self._disk_write.peak_rate / _MB

    @property
    def total_disk_read_gb(self) -> float:
        return self._disk_read.total / _GB

    @property
    def total_disk_write_gb(self) -> float:
        return self._disk_write.total / _GB

    @property
    def metrics(self) -> JournalistMetrics:
        mem = self._mem()
        # When the sampler never folded a sample (psutil unavailable, or context
        # exited before the first tick), reporting None is more honest than 0.
        elapsed_seconds = max(self._last_wall - self._start_wall, 0.0) if self._started else None
        return JournalistMetrics(
            peak_rss_gb=mem.peak / 1024,
            avg_rss_gb=mem.avg / 1024,
            peak_cpu_cores=self.peak_cpu_cores,
            avg_cpu_cores=self.avg_cpu_cores,
            peak_recv_mbps=self.peak_recv_mbps,
            total_recv_gb=self.total_recv_gb,
            peak_sent_mbps=self.peak_sent_mbps,
            total_sent_gb=self.total_sent_gb,
            peak_disk_read_mbps=self.peak_disk_read_mbps,
            total_disk_read_gb=self.total_disk_read_gb,
            peak_disk_write_mbps=self.peak_disk_write_mbps,
            total_disk_write_gb=self.total_disk_write_gb,
            elapsed_seconds=elapsed_seconds,
        )

    def __enter__(self) -> "Journalist":
        if not self._enabled:
            return self

        _SAMPLER.register(self)
        return self

    def _log_final_summary(self) -> None:
        # Parens format matches the periodic line's "avg/peak" grammar; "cur"
        # is dropped because it is meaningless at exit. For NET we lead with
        # peak (sustained throughput is captured in total GB, so a recap avg
        # rate adds nothing).
        m = self.metrics
        elapsed_min = (self._last_wall - self._start_wall) / 60
        net_part = ""
        if _SAMPLER._net_available:
            net_part = (
                f" | {_YELLOW}NET {m.peak_recv_mbps:.0f}\u2193 {m.peak_sent_mbps:.0f}\u2191 peak MBps;"
                f" total GB {m.total_recv_gb:.1f}\u2193 {m.total_sent_gb:.1f}\u2191{_RESET}"
            )
        disk_part = ""
        if _SAMPLER._disk_available:
            disk_part = (
                f" | {_MAGENTA}DISK {m.peak_disk_read_mbps:.0f}r {m.peak_disk_write_mbps:.0f}w peak MBps;"
                f" total GB {m.total_disk_read_gb:.1f}r {m.total_disk_write_gb:.1f}w{_RESET}"
            )
        _logger.info(
            "%s [%.1fm]: %sMEM avg %.1f / %.1f peak GB%s | %sCPU avg %.1f / %.1f peak%s%s%s",
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
            disk_part,
        )

    def __exit__(self, *exc: ty.Any) -> None:
        if not self._enabled:
            return

        # Clear _active first so any concurrent fold self-vetoes, then deregister
        # (no new snapshot will include us) and log the final summary last.
        self._active = False
        _SAMPLER.deregister(self)
        if self._sample_count > 0:
            self._log_final_summary()
