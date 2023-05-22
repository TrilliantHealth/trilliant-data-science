"""Intentionally unsophisticated memory and CPU profiler that is
triggered by logging.  Used for sanity-checking against the results
provided by other, black-box memory profilers.

Essentially, this can be added to any logger to force a sample of
memory and CPU usage on every logging statement, with the context
provided by the logger.

Since logging often happens at opportune times anyway, this is a
fairly easy and low-overhead way of getting the 'history' of your
process to be output within and alongside the logs.

This will do nothing if you don't have psutil installed.
If you do have psutil installed, it will be enabled automatically,
but without any patched loggers, you'll get no profiling output.

You can patch your loggers or wrap them.

Patching is implicit, and will try to intercept all calls you make to
`core.getLogger`. You must import this module before any others and
call the monkey_patch_core_getLogger function. Alternatively, you can
set the TH_PROF_ALL_LOGGERS environment variable and this
monkey-patching will be done automatically.

To wrap a logger, simply use the output of `wrap_logger(YOUR_LOGGER)`
as your logger. It will automatically output profiling information on
every usage.
"""
import contextlib
import csv
import logging
import os
import pathlib
import random
import string
import sys
import typing as ty
from datetime import datetime
from timeit import default_timer

from thds.core.stack_context import StackContext

F = ty.TypeVar("F", bound=ty.Callable)
Decorator = ty.Callable[[F], F]

try:
    import psutil  # type: ignore
except ImportError:
    psutil = None


def corelog():
    """We need core.log's basicConfig call to not happen on startup"""
    import thds.core.log as log

    return log


_IS_ENABLED = bool(psutil)

_PROF = "PROF>"
_MSG = "MSG>"
_TAG_LEN = 6


_TRUE_START = default_timer()


def _get_time():
    return default_timer() - _TRUE_START


class NoPsutilError(Exception):
    pass


def _get_proc(pid=-1, cache=dict()):  # noqa: B006
    """If we want CPU statistics, we need to cache these things"""
    if not psutil:
        raise NoPsutilError("")
    if pid < 0:
        pid = os.getpid()
    if pid not in cache:
        cache[pid] = psutil.Process(pid)
    return cache[pid]


def _get_mem_mb() -> float:
    try:
        return _get_proc().memory_info().rss / 10**6
    except NoPsutilError:
        return 0.0


def _get_cpu_percent(pid: int = -1) -> float:
    try:
        proc = _get_proc(pid)
        return proc.cpu_percent() + sum(_get_cpu_percent(c.pid) for c in proc.children())
    except NoPsutilError:
        return 0.0


_PID: int = 0
_CSV_OUT = None
_CSV_WRITER = None
_PROFS_DIR = pathlib.Path("th-profiles")


def _open_csv_writer():
    global _PID, _CSV_OUT, _CSV_WRITER
    cur_pid = os.getpid()
    if _PID != cur_pid:  # we have forked a new process - open a new file
        parent_pid = "" if _PID == 0 else f"{_PID}_PARENT_"
        _PROFS_DIR.mkdir(exist_ok=True)
        csvf = _PROFS_DIR / (
            f"th-prof-{datetime.utcnow().isoformat()}-"
            f"{'-'.join(sys.argv).replace('/', '_')}___{parent_pid}{cur_pid}.csv"
        )
        _CSV_OUT = csvf.open("w")
        _CSV_WRITER = csv.writer(_CSV_OUT)
        _CSV_WRITER.writerow(("tag", "time", "mem", "cpu", "time_d", "mem_d", "descriptors", "msg"))
        _PID = cur_pid


def _write_record(*row: str) -> None:
    if _IS_ENABLED:
        _open_csv_writer()
        _CSV_WRITER.writerow(row)  # type: ignore
        _CSV_OUT.flush()  # type: ignore


def _dt(dt: float) -> str:
    return f"{dt:+9.1f}s"


def _t(t: float) -> str:
    return f"{t:9.1f}s"


def _cpu(cpu: float) -> str:
    return f"{cpu:7.1f}%"


def _dm(dm: float) -> str:
    return f"{dm:+9.1f}m"


def _m(m: float) -> str:
    return f"{m:9.1f}m"


class ThProfiler:
    """A ContextManager that outputs time and memory on enter, exit, and
    whenever it is called in between.

    This is not designed for tight loops - it makes kernel calls, and
    therefore is most suitable for attaching to relatively beefy parts
    of your program that you want general purpose, basic profiling
    info logged for.

    It _is_ designed to be used in concert with `scope.enter` - using
    with statements for profiling code that might later be removed is
    very ugly and a Bad Idea™.
    """

    def __init__(self, tag: str):
        self.tag = tag
        self.start_mem = _get_mem_mb()
        self.start_t = _get_time()

    def _desc(self, **more_desc) -> str:
        return "; ".join([f"{k}: {v}" for k, v in dict(corelog()._LOG_CONTEXT(), **more_desc).items()])

    def _parts(self) -> ty.Tuple[str, ...]:
        now_m = _get_mem_mb()
        now_t = _get_time()
        now_cpu = _get_cpu_percent()
        delta_t = now_t - self.start_t
        delta_m = now_m - self.start_mem
        return _t(now_t), _m(now_m), _cpu(now_cpu), _dt(delta_t), _dm(delta_m)

    def _make_msg(self, msg: str, **more_desc) -> ty.Tuple[str, ...]:
        descriptors = self._desc(**more_desc)
        parts = self._parts()
        _write_record(self.tag, *parts, descriptors, msg)
        if msg:
            msg = _MSG + " " + msg
        return tuple(filter(None, (_PROF, self.tag, *parts, descriptors, msg)))

    def __call__(self, msg: str = "", **descriptors) -> str:
        """Outputs the delta between now and when this ThProfiler was created"""
        if not _IS_ENABLED:
            return msg
        return " ".join(self._make_msg(msg, **descriptors))


_TH_PROFILER = StackContext(
    "__th_profiler",
    ThProfiler("." * _TAG_LEN)
    # This root profiler exists simply to make sure that there's
    # always a profiling context even if no function has been
    # annotated with `context`.
)


def _make_tag(parent: str = "", _cache: dict = {None: 0}) -> str:  # noqa: B006
    # try to cycle through string prefixes for reasonability
    try:
        src = string.ascii_uppercase
        ascending_prefix = src[_cache[None] % len(src)]
        if parent:
            return ascending_prefix + parent[:-1]
        end = "".join(random.choice(src) for _ in range(_TAG_LEN - 1))
        return ascending_prefix + end
    finally:
        _cache[None] += 1  # increment prefix


@contextlib.contextmanager
def profile(profname: str, tag: str = ""):
    """Establish a new profiling context around a given function, with its
    own 'start values' for time and memory, and its own 'tag' for
    identifiability.
    """

    if not _IS_ENABLED:
        yield
        return

    with corelog().logger_context(profname=profname):
        with _TH_PROFILER.set(ThProfiler(tag or _make_tag(_TH_PROFILER().tag))):
            yield


class ProfilingLoggerAdapter(logging.LoggerAdapter):
    """Use this to replace your current logger - it remains a logger, but now prepends profiling information!"""

    def process(self, msg, kwargs):
        kw_logger = corelog().KwLogger(self.logger, dict())
        msg, kwargs = kw_logger.process(msg, kwargs)
        if not _IS_ENABLED:
            return msg, kwargs
        extra = kwargs.get("extra") or dict()
        th_kw = extra.get(corelog()._TH_REC_CTXT) or dict()
        return _TH_PROFILER()(msg, **th_kw), kwargs  # type: ignore


def wrap_logger(logger: logging.Logger) -> ProfilingLoggerAdapter:
    return ProfilingLoggerAdapter(logger, dict())


def monkey_patch_core_getLogger():
    """Only call this if you want all your loggers replaced by profiling loggers.

    For this to take effect, you must call it *before* imports of modules that define loggers.
    """
    if not _IS_ENABLED:
        return
    import thds.core.log

    thds.core.log.__dict__["getLogger"] = lambda name: wrap_logger(logging.getLogger(name))


if "TH_PROF_ALL_LOGGERS" in os.environ:
    if not _IS_ENABLED:
        print(
            "Warning: psutil is not installed but you have set TH_PROF_ALL_LOGGERS. Please pip install psutil"
        )
    else:
        print("Monkey patching all loggers because of TH_PROF_ALL_LOGGERS")
        logging.basicConfig(
            level=os.environ.get("LOGLEVEL", logging.INFO),
            style="{",
            format="{name:<45} - {levelname:^8} - {message}",
        )
        monkey_patch_core_getLogger()
