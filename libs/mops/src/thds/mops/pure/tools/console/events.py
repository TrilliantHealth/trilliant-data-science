"""Event vocabulary for live run observation.

Distinct from `..summarize`, which records one file per *completed* invocation. These
events also mark work as it is invoked, so a reader can see what is in flight rather
than only what has finished.

Events are facts about an invocation, keyed by (invocation_key, event type). They are
never deleted and never contradict each other, so folding them into run state is
idempotent and order-independent - which is what allows any reader to publish a snapshot
without coordinating with other readers.

Payloads carry references, never contents: URIs are recorded, argument and return values
never are.
"""

import datetime as dt
import typing as ty

from ...core.memo import function_memospace
from . import runtime

EventType = ty.Literal["run_started", "invoked", "started", "completed", "failed"]


class Event(ty.TypedDict, total=False):
    """One append to the run's event log.

    `invocation_key` is the memo URI minus its blob root - storage-agnostic, stable
    across machines, and the join key for everything a reader assembles.
    """

    event: EventType
    at: str  # ISO8601 UTC
    run_name: str
    # the orchestrator run this belongs to - `summarize.run_summary.RUN_NAME`, shared with
    # the summary tree so the two views of one run can be correlated. Deliberately not
    # `run_id`, which mops already uses for a per-invocation id generated on the remote.
    invocation_key: str

    function_name: str  # "module:function"
    pipeline_id: str
    attempt_id: str  # the lease writer_id - distinguishes retries of one invocation

    parent_invocation_key: str  # nesting; empty at the top level
    was_error: bool

    started_at: str  # ISO8601 UTC
    # on a terminal event: when the attempt that produced this result began. The `started`
    # events say when each attempt began; this says which of them was the one that counted.

    invoked_at: str  # ISO8601 UTC
    # when the orchestrator invoked this, repeated by the remote from the metadata it was
    # handed. The orchestrator says so itself in its own `invoked` event, but that event
    # travels to shared storage in a batch on a timer - so an orchestrator killed outright
    # takes its last invocations with it. Anything that reached a remote carries its own
    # invocation time here, and so keeps its queue wait no matter what became of the
    # process that started it.

    runtime: str
    where: dict[str, str]
    # what ran the work and where to find it, reported by the remote itself - the only
    # party that knows, and only while it runs: the orchestrator names a Job, but which
    # pod served it after retries or preemption is settled on the far side, and a pod is
    # gone minutes after it exits.
    #
    # `runtime` names the launching runtime ("k8s", "databricks"); `where` is whatever
    # addresses an execution within it, supplied by that runtime's own provider (see
    # `runtime.RuntimeContext`). Open rather than enumerated so a runtime mops has never
    # heard of still reports, and a reader that does not recognise one still has key/value
    # pairs to show.

    memo_uri: str
    # the invocation's address in the blob store, and so where its pickled arguments and
    # result live. Unlike `invocation_key` this includes the blob root, which is exactly
    # what makes it fetchable and what makes it unusable as an identity across roots.
    was_memoized: bool  # a cache hit: this run never invoked it and never ran it

    original_invoked_at: str
    original_started_at: str
    original_ended_at: str
    # when the work behind a memoized result actually happened, from the metadata stored
    # beside it. Named apart from `invoked_at`/`started_at` so a reader places the hit at
    # the moment it was served by default, and can reconstruct the original computation's
    # timeline when asked to.

    error: str
    # one line, for a failure the remote never got to record itself - the shim was
    # rejected, or it exited without writing anything. A remote that raises pickles the
    # exception to its memo URI, and a reader should go there for the traceback.


class _Parsed(ty.NamedTuple):
    invocation_key: str
    pipeline_id: str
    function_name: str


def invocation_key_of(memo_uri: str, runner_prefix: str = "") -> str:
    """The storage-agnostic identity of one invocation.

    Shared with the summary reader so an event and a summary for the same invocation join
    without either having to know how the other was produced.
    """
    return _parse(memo_uri, runner_prefix).invocation_key


def _parse(memo_uri: str, runner_prefix: str = "") -> _Parsed:
    """Decompose a memo URI, falling back to the raw URI as the key if it won't parse.

    The fallback keeps a malformed URI from costing an event; a reader that can't split
    it still has something unique to group by.
    """
    try:
        parts = function_memospace.parse_memo_uri(memo_uri, runner_prefix)
    except (ValueError, AssertionError):
        return _Parsed(memo_uri, "", "")

    function_name = f"{parts.function_module}:{parts.function_name}"
    return _Parsed(
        "/".join((parts.pipeline_id, function_name, parts.args_hash)),
        parts.pipeline_id,
        function_name,
    )


def _event(kind: EventType, memo_uri: str, attempt_id: str, at: dt.datetime) -> Event:
    parsed = _parse(memo_uri)
    return Event(
        event=kind,
        at=at.isoformat(),
        invocation_key=parsed.invocation_key,
        pipeline_id=parsed.pipeline_id,
        function_name=parsed.function_name,
        attempt_id=attempt_id,
        memo_uri=memo_uri,
    )


def invoked(memo_uri: str, *, attempt_id: str, at: dt.datetime) -> Event:
    """Handed to the shim. Pairs with `started` to measure queue wait."""
    return _event("invoked", memo_uri, attempt_id, at)


def started(
    memo_uri: str,
    *,
    attempt_id: str,
    at: dt.datetime,
    where: runtime.RuntimeContext = runtime.EMPTY,
    invoked_at: str = "",
) -> Event:
    """The remote process began. The interval since `invoked` is time spent waiting to be
    scheduled - image pull, node scale-up, preemption - which neither side can measure
    alone.

    `where` is recorded here rather than derived later because it is only knowable here,
    and only now - see `runtime`. It is passed in rather than read, so that this stays a
    pure description of an event and the remote entrypoint decides what describes it.

    `invoked_at` repeats what the orchestrator already said, from the metadata this remote
    was handed. It costs a few bytes on an object that was being written anyway, and means
    an invocation that reached a remote keeps its queue wait even if the orchestrator died
    before publishing the invocation.
    """
    event = _event("started", memo_uri, attempt_id, at)
    if where.runtime:
        event["runtime"] = where.runtime
    if where.coordinates:
        event["where"] = dict(where.coordinates)
    if invoked_at:
        event["invoked_at"] = invoked_at

    return event


def finished(
    memo_uri: str,
    *,
    attempt_id: str,
    at: dt.datetime,
    was_error: bool,
    run_name: str = "",
    started_at: str = "",
) -> Event:
    """The invocation produced a result or an exception.

    `started_at` is this attempt's own start, stated rather than left to be inferred. An
    invocation that was evicted and retried has more than one `started` event, and only
    the attempt that finished knows which one belongs to the work that produced the result.
    """
    event = _event("failed" if was_error else "completed", memo_uri, attempt_id, at)
    event["was_error"] = was_error
    if started_at:
        event["started_at"] = started_at
    if run_name:
        event["run_name"] = run_name

    return event


def memoized(
    memo_uri: str,
    *,
    at: dt.datetime,
    was_error: bool = False,
    invoked_at: str = "",
    started_at: str = "",
    ended_at: str = "",
) -> Event:
    """A result served from the cache, with when its work originally ran.

    `at` is the moment this run was answered. The `original_*` timestamps come from the
    result's stored metadata and describe the computation that produced it - possibly in
    another run, possibly long ago. Without this event, a memoized hit is visible only in
    the orchestrator's local summary files, and a remote observer sees a run where the
    invocation never existed at all.
    """
    event = _event("failed" if was_error else "completed", memo_uri, "", at)
    event["was_memoized"] = True
    event["was_error"] = was_error
    for field, value in (
        ("original_invoked_at", invoked_at),
        ("original_started_at", started_at),
        ("original_ended_at", ended_at),
    ):
        if value:
            event[field] = value  # type: ignore[literal-required]

    return event


def failed(memo_uri: str, *, attempt_id: str, at: dt.datetime, error: str) -> Event:
    """The orchestrator gave up on an invocation the remote never reported on.

    Distinct from `finished(was_error=True)`, which is a remote saying its function raised.
    These are the failures with no remote to report them - the shim was rejected, the pod
    exited without writing a result, the invocation could not be uploaded. Without this
    event such an invocation stays `invoked` forever, which reads as 'still waiting for a
    pod' - the one state it is not in.
    """
    event = _event("failed", memo_uri, attempt_id, at)
    event["was_error"] = True
    event["error"] = error
    return event
