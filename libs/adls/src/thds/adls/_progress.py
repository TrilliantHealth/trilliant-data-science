"""An app-global progress reporter that attempts to reduce the number
of progress reports using either a time delay or by using fancy
progress bars.
"""

import os
import typing as ty
from functools import reduce
from timeit import default_timer

from thds.core import log

logger = log.getLogger(__name__)
_1MB = 2**20
_UPDATE_INTERVAL_S = 5
_SUPPORTS_CR = not bool(os.getenv("CI"))
# CI does not support carriage returns.
# if we find other cases that don't, we can add them here.


class ProgressState(ty.NamedTuple):
    start: float
    total: int
    n: int


def _dumb_report_progress(desc: str, state: ProgressState):
    if not state.total:
        logger.info(f"{desc} complete!")
        return
    if not state.n:
        return  # don't report when nothing has happened yet.

    start, total, n_bytes = state
    pct = 100 * (n_bytes / total)
    elapsed = default_timer() - start
    rate_s = f" at {n_bytes/_1MB/elapsed:,.1f} MiB/s"
    logger.info(f"{desc}: {n_bytes:,} / {total:,} bytes ({pct:.1f}%){rate_s} in {elapsed:.1f}s")


def _sum_ps(ps: ty.Iterable[ProgressState]) -> ProgressState:
    return reduce(
        lambda x, y: ProgressState(min(x.start, y.start), x.total + y.total, y.n + x.n),
        ps,
        ProgressState(default_timer(), 0, 0),
    )


def _blobs(n: list) -> str:
    if not n:
        return ""
    return f" {len(n)} blob" + ("" if len(n) == 1 else "s")


class _Reporter(ty.Protocol):
    def __call__(self, states: ty.List[ProgressState]):
        ...


class DumbReporter:
    def __init__(self, desc: str):
        self._desc = desc
        self._started = default_timer()
        self._last_reported = self._started

    def __call__(self, states: ty.List[ProgressState]):
        now = default_timer()
        # two cases that require a report:
        # 1. it's been a long enough time (update interval) since the last report.
        if now - self._last_reported > _UPDATE_INTERVAL_S:
            _dumb_report_progress(self._desc + f" {_blobs(states)}", _sum_ps(states))
            self._last_reported = now
        # 2. a download finished _and_ that specific download took longer overall than our update interval.
        else:
            for state in states:
                if (
                    state.total
                    and state.n >= state.total  # download finished
                    and (now - state.start) > _UPDATE_INTERVAL_S  # and it took a while
                ):
                    # report individually for each download that finished.
                    _dumb_report_progress(self._desc + f" {_blobs([state])}", state)
            # notably, we do not delay the next 'standard' report because of downloads finishing.


class TqdmReporter:
    """Falls back to DumbReporter if tqdm is not installed."""

    def __init__(self, desc: str):
        self._desc = desc
        self._bar = None
        self._dumb = DumbReporter(desc)

    def __call__(self, states: ty.List[ProgressState]):
        try:
            from tqdm import tqdm  # type: ignore

            bar = self._bar
            state = _sum_ps(states)
            if not bar and state.total > 0:
                bar = tqdm(
                    total=state.total,
                    delay=_UPDATE_INTERVAL_S,
                    mininterval=_UPDATE_INTERVAL_S,
                    initial=state.n,
                    unit="byte",
                    unit_scale=True,
                )  # type: ignore
            if bar:
                # if there are zero active states (which is possible),
                # n and total will be zero after sum, and we don't
                # want to set zeros on an existing non-zero bar.
                bar.total == state.total or bar.total
                new_n = state.n or bar.n
                bar.update(new_n - bar.n)
                bar.desc = f"{self._desc}{_blobs(states)}"
                if _SUPPORTS_CR:
                    bar.refresh()

                if bar.n >= bar.total:
                    bar.close()
                    bar = None

                self._bar = bar
        except ModuleNotFoundError:
            self._dumb(states)


class Tracker:
    def __init__(self, reporter: _Reporter):
        self._progresses: ty.Dict[str, ProgressState] = dict()
        self._reporter = reporter

    def add(self, key: str, total: int) -> ty.Tuple["Tracker", str]:
        if total < 0:
            total = 0
        self._progresses[key] = ProgressState(default_timer(), total, 0)
        self._reporter(list(self._progresses.values()))
        return self, key

    def __call__(self, key: str, written: int):
        assert written >= 0, "cannot write negative bytes: {written}"
        try:
            start, total, n = self._progresses[key]
            self._progresses[key] = ProgressState(start, total, n + written)
            self._reporter(list(self._progresses.values()))
            if self._progresses[key].n >= total:
                del self._progresses[key]
        except KeyError:
            self._reporter(list(self._progresses.values()))


_GLOBAL_DN_TRACKER = Tracker(TqdmReporter("thds.adls downloading"))
_GLOBAL_UP_TRACKER = Tracker(TqdmReporter("thds.adls uploading"))
T = ty.TypeVar("T", bound=ty.IO)


def _proxy_io(io_type: str, stream: T, key: str, total_len: int) -> T:
    assert io_type in ("read", "write"), io_type

    try:
        old_io = getattr(stream, io_type)
        total_len = total_len or len(stream)  # type: ignore
    except (AttributeError, TypeError):
        return stream

    if io_type == "read":
        tracker, _ = _GLOBAL_UP_TRACKER.add(key, total_len)
    else:
        tracker, _ = _GLOBAL_DN_TRACKER.add(key, total_len)

    def io(data_or_len: ty.Union[bytes, int]):
        r = old_io(data_or_len)
        io_len = (
            total_len
            if data_or_len == -1
            else (len(data_or_len) if isinstance(data_or_len, bytes) else data_or_len)
        )
        tracker(key, io_len)
        return r

    setattr(stream, io_type, io)
    return stream


def report_download_progress(stream: T, key: str, total: int = 0) -> T:
    if not total:  # if we don't know how big a download is, we can't report progress.
        return stream
    return _proxy_io("write", stream, key, total)


def report_upload_progress(stream: T, key: str, total: int = 0) -> T:
    return _proxy_io("read", stream, key, total)
