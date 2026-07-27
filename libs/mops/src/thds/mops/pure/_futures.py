"""A mops-owned future that delegates to an inner PFuture and additionally
carries the invocation's memo URI (known at submit) and its ResultMetadata
(populated when the result is unwrapped). Stays picklable: mops creates futures
across process-pool boundaries, so the whole object is pickled."""

import typing as ty
from dataclasses import dataclass

from thds.core import futures, log

from .core.metadata import ResultMetadata

logger = log.getLogger(__name__)

R = ty.TypeVar("R")


@dataclass
class _MetadataCapturingCallback(ty.Generic[R]):
    """Module-level picklable done-callback wrapper registered on the inner
    tuple-future. When the tuple-future resolves it (a) stores the metadata on
    the parent MopsFuture and (b) invokes the user callback with the parent
    MopsFuture, whose .result() yields the VALUE (not the tuple)."""

    mops_future: "MopsFuture[R]"
    user_fn: ty.Callable[["futures.PFuture[R]"], None]

    def __call__(self, _tuple_future: "futures.PFuture[tuple[R, ty.Optional[ResultMetadata]]]") -> None:
        # Only a SUCCESSFUL invocation carries a metadata tuple. Reading one from a
        # failed invocation re-raises its exception, and from a cancelled one raises
        # CancelledError (`PFuture` documents both as possible and deliberately omits
        # `cancelled()`, so there is nothing cheaper to test than the read itself).
        # Either would propagate out of this callback into the inner future's dispatch
        # loop, which logs the raising callback and moves on to the next - so
        # `user_fn` would never run, and whoever registered it would wait forever for
        # a completion that had in fact already happened. Metadata is therefore
        # best-effort and `user_fn` always runs; the outcome (value or exception)
        # still reaches the caller through the MopsFuture it receives.
        try:
            self.mops_future._capture_metadata_from(_tuple_future)
        except Exception as exc:
            logger.debug("no result metadata for %s: %s", self.mops_future.memo_uri, exc)
        except BaseException as exc:  # noqa: B036 - CancelledError; delivered via user_fn below
            logger.debug("invocation %s was cancelled: %s", self.mops_future.memo_uri, exc)
        self.user_fn(self.mops_future)


class MopsFuture(ty.Generic[R]):
    """Wraps an inner PFuture and adds `memo_uri` (set at construction) and
    `result_metadata` (None until the result is unwrapped).

    Two construction modes coexist (see `__init__` vs `from_tuple_future`):

    - plain value-future: `_inner` yields the value directly; metadata is set
      explicitly by the caller via `set_result_metadata`. Used by the eager
      memo-hit / synchronous-shim paths in `runner/local.py`.
    - tuple-future: `_inner` yields `(value, metadata)`. The protocol methods
      translate per-call WITHOUT forcing resolution, so `add_done_callback`
      registers a wrapper on the inner future and returns immediately rather
      than blocking on the underlying (possibly multi-minute) computation.
    """

    def __init__(self, inner: futures.PFuture[R], memo_uri: str) -> None:
        self._inner = inner
        self._yields_tuple = False
        self.memo_uri = memo_uri
        self.result_metadata: ty.Optional[ResultMetadata] = None

    @classmethod
    def from_tuple_future(
        cls,
        tuple_future: futures.PFuture[tuple[R, ty.Optional[ResultMetadata]]],
        memo_uri: str,
    ) -> "MopsFuture[R]":
        """Construct a MopsFuture from a future that yields (value, metadata).

        The returned MopsFuture's .result() returns just the value and populates
        .result_metadata as a side effect; .add_done_callback registers on the
        inner tuple-future without resolving it, so it returns promptly even
        when the underlying computation has not finished.
        """
        mf: MopsFuture[R] = cls(ty.cast("futures.PFuture[R]", tuple_future), memo_uri)
        mf._yields_tuple = True
        return mf

    def set_result_metadata(self, md: ty.Optional[ResultMetadata]) -> None:
        self.result_metadata = md

    def _capture_metadata_from(
        self, tuple_future: "futures.PFuture[tuple[R, ty.Optional[ResultMetadata]]]"
    ) -> None:
        """Idempotent: same resolved tuple always yields the same metadata, so
        capturing it from both .result() and the done-callback is safe."""
        self.result_metadata = tuple_future.result()[1]

    def running(self) -> bool:
        return self._inner.running()

    def cancel(self) -> bool | None:
        """Cancel the underlying invocation, delegating down the future chain
        (`_ChainedFuture` -> `LazyFuture` -> the runtime shim's future, which
        for k8s deletes the Job). Tri-state: True (cancelled), False (couldn't -
        already done/raced), None (the inner doesn't support cancellation, e.g.
        a memo hit or a synchronous shim). The public home for cancellation
        because a caller already holds the concrete `MopsFuture`."""
        return futures.try_cancel(self._inner)

    def done(self) -> bool:
        return self._inner.done()

    def result(self, timeout: ty.Optional[float] = None) -> R:
        if not self._yields_tuple:
            return self._inner.result(timeout)

        tuple_future = ty.cast("futures.PFuture[tuple[R, ty.Optional[ResultMetadata]]]", self._inner)
        value, md = tuple_future.result(timeout)
        self.result_metadata = md
        return value

    def exception(self, timeout: ty.Optional[float] = None) -> ty.Optional[BaseException]:
        return self._inner.exception(timeout)

    def add_done_callback(self, fn: ty.Callable[["futures.PFuture[R]"], None]) -> None:
        if not self._yields_tuple:
            self._inner.add_done_callback(fn)
            return

        # register a wrapper on the tuple-future: it captures metadata and hands
        # the user `self` (whose .result() yields the value). This must NOT force
        # resolution of the inner future.
        ty.cast("futures.PFuture[tuple[R, ty.Optional[ResultMetadata]]]", self._inner).add_done_callback(
            _MetadataCapturingCallback(mops_future=self, user_fn=fn)
        )

    def set_result(self, result: R) -> None:
        self._inner.set_result(result)

    def set_exception(self, exception: BaseException) -> None:
        self._inner.set_exception(exception)
