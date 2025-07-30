import collections
import threading
import time
import typing as ty

# we use concurrent.futures.Future as an implementation detail, but it's communicated
# as core.futures.PFuture to give us the flexibility to change the implementation later if needed.
from concurrent.futures import Future
from dataclasses import dataclass
from uuid import uuid4

from typing_extensions import Self

from thds import core

R_0 = ty.TypeVar("R_0", contravariant=True)  # R-naught - the thing that might resolve a Future.
# a value for this type may never be None.

R = ty.TypeVar("R")
# the Result type of the Future. These are allowed to be None, since some Futures may
# resolve but not return a value.


class NotYetDone:
    pass


_LastSeenAt = float  # type alias for the last seen time of the Future, in seconds since epoch


FutureInterpreter = ty.Callable[[ty.Optional[R_0], _LastSeenAt], ty.Union[R, NotYetDone]]
# a FutureInterpreter is a function that takes an object R_0 and the time.monotonic() at
# which it was last seen, and returns either NotYetDone (if the status is still in progress) or
# the actual Future result of type R, or, if the status is failure,
# _raises_ an appropriate Exception.


class _FutureInterpretationShim(ty.Generic[R_0, R]):
    def __init__(self, interpreter: FutureInterpreter[R_0, ty.Union[NotYetDone, R]]) -> None:
        self.future = Future[R]()
        self._interpreter = interpreter
        self._id = uuid4().hex  # has an id so it can be hashed and therefore easily found in a set

    def __hash__(self) -> int:
        return hash(self._id)

    def __call__(self, r_0: ty.Optional[R_0], last_seen_at: float) -> ty.Optional[Self]:
        """First and foremost - this _must_ be treated as an object that the creator
        is ultimately responsible for calling on a semi-regular basis. It represents a
        likely deadlock for the holder of the Future if it is never called.

        Return False if the Future is still in progress and should not be unregistered.
        Return True if the Future is done and should be unregistered.
        """
        try:
            interpretation = self._interpreter(r_0, last_seen_at)
            if isinstance(interpretation, NotYetDone):
                return None  # do nothing and do not unregister - the status is still in progress.

            self.future.set_result(interpretation)
        except Exception as e:
            self.future.set_exception(e)

        return self


K = ty.TypeVar("K")  # Key type for the UncertainFuturesTracker


@dataclass
class _FuturesState(ty.Generic[R_0]):
    """Represents a single 'observable' that may have multiple Futures (and therefore interpretations) associated with it."""

    futshims: list[_FutureInterpretationShim[R_0, ty.Any]]
    last_seen_at: float


def official_timer() -> float:
    # we don't need any particular meaning to the time.
    return time.monotonic()


class UncertainFuturesTracker(ty.Generic[K, R_0]):
    """This class represents a kind of Future where we cannot be guaranteed that we will ever see
    any further information about it, because we do not control the source of the data.

    A good example would be a Kubernetes object that we are watching - we may _think_ that a Job will be created,
    but there are race conditions galore in terms of actually looking for that object.

    However, if we _do_ see it at a some point, then we can interpret future 'missingness'
    as a tentative success.

    The danger with this uncertainty is that Futures represent implicit deadlocks - if we
    never resolve the Future, then a caller may be waiting for it forever. Therefore, we
    ask the original requestor of the Future to specify how long they are willing to wait
    to get a result, after which point we will resolve the Future as an exception.
    """

    def __init__(self, allowed_stale_seconds: float) -> None:
        self._keyed_futures_state = collections.OrderedDict[K, _FuturesState[R_0]]()
        self._lock = threading.Lock()  # i don't trust ordered dict operations to be thread-safe.
        self._check_stale_seconds = allowed_stale_seconds

    def create(self, key: K, interpreter: FutureInterpreter[R_0, R]) -> core.futures.PFuture[R]:
        futshim = _FutureInterpretationShim(interpreter)
        with self._lock:
            if key not in self._keyed_futures_state:
                self._keyed_futures_state[key] = _FuturesState(
                    [futshim],
                    last_seen_at=official_timer() + self._check_stale_seconds,
                    # we provide a double margin for objects that we have never seen before.
                )
                self._keyed_futures_state.move_to_end(key, last=False)
                # never seen and therefore should be at the beginning (most stale)
            else:
                # maintain our ordered dict so we can handle garbage collection of stale Futures.
                self._keyed_futures_state[key].futshims.append(futshim)

        return futshim.future

    def update(self, key: ty.Optional[K], r_0: ty.Optional[R_0]) -> None:
        """Update the keyed Futures based on their interpreters.

        Also check any stale Futures - Futures that have not seen an update (via their key) in a while.

        If `key` is None, we will update all Futures that have been created so far.
        """

        def check_resolution(fut_state: _FuturesState[R_0], inner_r_0: ty.Optional[R_0]) -> None:
            for future_shim_that_is_done in core.parallel.yield_results(
                [
                    core.thunks.thunking(futshim)(inner_r_0, fut_state.last_seen_at)
                    for futshim in fut_state.futshims
                ],
                progress_logger=core.log.getLogger(__name__).debug,
                named="UncertainFuturesTracker.update",
            ):
                if future_shim_that_is_done is not None:
                    # the Future is done, so we can remove it from the list of Futures.
                    fut_state.futshims.remove(future_shim_that_is_done)

        if key is not None:
            with self._lock:
                if key not in self._keyed_futures_state:
                    self._keyed_futures_state[key] = _FuturesState(list(), last_seen_at=official_timer())
                else:
                    # maintain our ordered dict so we can handle garbage collection of stale Futures.
                    self._keyed_futures_state.move_to_end(key)
                    self._keyed_futures_state[key].last_seen_at = official_timer()

            fut_state = self._keyed_futures_state[key]
            check_resolution(fut_state, r_0)

        # 'garbage collect' any Futures that haven't been updated in a while.
        for futs_state in self._keyed_futures_state.values():
            if futs_state.last_seen_at + self._check_stale_seconds < official_timer():
                check_resolution(futs_state, None)
            else:  # these are ordered, so once we see one that's not stale, we can stop checking.
                # this prevents us from having to do O(N) checks for every update.
                break
