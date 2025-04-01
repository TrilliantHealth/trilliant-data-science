"""
A module that adds a simple interface for tracking execution times of various pieces of code.

```python
tracker = TimeTracker()

@tracker.track()
def computation(num: int):
    ...

with tracker(tracker.total):
    for i in range(10):
        with tracker("setup"):
            ...
            ...
        computation(i)

print(tracker.percentage_of_totals)
```

`TimeTracker`s can also be merged.

*module_1.py*
```python

tracker = TimeTracker()

@tracker.track()
def foo():
    ...

@tracker.track()
def bar():
    ...


@tracker.track()
def main():
    foo()
    bar()
```

*module_2.py*
```python
import time
import module_1 as mod1

tracker = TimeTracker()

with tracker(tracker.total):
    for i in range(100):
        time.sleep(5)
    mod1.main()

tracker.merge(mod1.tracker)
```
"""

import json
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from functools import wraps
from typing import (
    Callable,
    DefaultDict,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import ParamSpec

from thds.core import config, log

T = TypeVar("T")
P = ParamSpec("P")

F = TypeVar("F", bound=Callable)


SINGLE_LINE_JSON_TIMERS = config.item(
    "unified.single-line-json-timers", default=False, parse=config.tobool
)
# setting this to true makes it much easier to extract the data from logs in a production run using Grafana.


def timer(func: F) -> F:
    """
    Decorator to add logging of timer information to a function invocation. Logs when entering a function and then logs
    with time information when exiting.
    :param func:
        Function to decorate with timing info.
    :return:
        Wrapped function.
    """
    logger = log.getLogger(func.__module__)

    @wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = time.time()
        start_formatted = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(start))

        logger.info("Starting %r at %s", func.__name__, start_formatted)
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time

        end = time.time()
        end_formatted = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(end))
        logger.info("Finished %r in %0.4f secs at %s", func.__name__, run_time, end_formatted)
        return value

    return cast(F, wrapper_timer)


@dataclass
class Timer:
    secs: float = 0.0
    calls: int = 0

    def __iter__(self) -> Generator[Tuple[str, Union[int, float]], None, None]:
        for k, v in {**asdict(self), "secs_per_call": self.secs_per_call}.items():
            yield k, v

    def __add__(self, other: "Timer") -> "Timer":
        return Timer(self.secs + other.secs, self.calls + other.calls)

    def __radd__(self, other):
        if other == 0:
            return self
        else:
            return self.__add__(other)

    @property
    def secs_per_call(self) -> float:
        return (self.secs / self.calls) / 60.0 if self.calls > 0 else float("nan")

    @property
    def mins(self) -> float:
        return self.secs / 60.0

    def pct_of_total(self, total: float) -> float:
        return (self.secs / total) * 100


class TimeTracker:
    total: str = "total"

    def __init__(self, times: Optional[DefaultDict[str, Timer]] = None):
        self.tracked_times: DefaultDict[str, Timer] = times or defaultdict(Timer)
        self._names: List[str] = []
        self._start_times: List[float] = []

    def reset(self) -> None:
        self.tracked_times = defaultdict(Timer)
        self._names = []
        self._start_times = []

    def to_json(self) -> Iterator[str]:
        if SINGLE_LINE_JSON_TIMERS:
            for name, timer in sorted(self.tracked_times.items(), key=lambda x: x[0]):
                yield json.dumps({name: dict(timer)}, indent=None)
        else:
            yield json.dumps(
                {name: dict(timer) for name, timer in self.tracked_times.items()},
                sort_keys=True,
                indent=4,
            )

    def track(self, component_name: Optional[str] = None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start = time.perf_counter()
                func_result = func(*args, **kwargs)
                cmpnt = self.tracked_times[component_name or f"{func.__module__}.{func.__qualname__}"]
                cmpnt.secs += time.perf_counter() - start
                cmpnt.calls += 1
                return func_result

            return wrapper

        return decorator

    def __call__(self, component_name: str) -> "TimeTracker":
        self._names.append(component_name)
        return self

    def __enter__(self) -> "TimeTracker":
        self._start_times.append(time.perf_counter())
        return self

    def __exit__(self, *args, **kwargs):
        cmpnt = self.tracked_times[self._names.pop()]
        cmpnt.secs += time.perf_counter() - self._start_times.pop()

    @property
    def pct_of_totals(self) -> Dict[str, float]:
        total = self.tracked_times.get(self.total)
        if total:
            return {
                name: timer.pct_of_total(total.secs)
                for name, timer in self.tracked_times.items()
                if name != "total"
            }
        return {}
