"""Inject precomputed results into the memoization store.

Sometimes a result already exists - computed outside mops, or reconstructible from
durable output - and you want future callers to memo-hit it instead of recomputing.
The tools here write such a value at a memo URI exactly as the remote entry would
have written a computed one: same result pickle, same metadata files, plus a
`precomputed_result=true` marker in the extra metadata so injected results remain
identifiable.

The memo URI for a call is only honestly derivable by running the orchestrator path
(argument serialization has content-addressing side effects), so both tools here are
shims: the orchestrator hands the memo URI to a shim only after the memo check has
missed, the invocation lock is held, and the invocation pickle has been uploaded -
meaning injection happens exactly where real computation would have, with full
provenance.

An injected result is thereafter indistinguishable from a computed one (aside from
the marker). The `make_value` callable bears full responsibility for constructing
*exactly* what the function would have returned, including types and any embedded
Sources or Paths.

WARNING - the memo key hashes pickle BYTES, and pickle bytes depend on object
IDENTITY, not just value: equal strings that are the same object pickle as memo
backreferences, distinct-but-equal objects pickle fresh. Arguments assembled by a
different code path than the real caller's can therefore produce a different key
for equal values, leaving the injected result unreachable. Inject from within the
REAL orchestration (whose calls mint the real keys) rather than re-deriving
arguments in a side script; if you must re-derive, verify reachability with a call
whose argument-assembly code path is the production one, not your own.
"""

import typing as ty
from datetime import datetime, timezone

from thds.core import log

from ..core import metadata, uris
from ..core.entry import route_return_value_or_exception
from ..core.types import Args, Kwargs
from ..runner.types import ShimBuilder, SyncShim
from . import mprunner, remote

logger = log.getLogger(__name__)

MakeValueThunk = ty.Callable[[ty.Callable, Args, Kwargs], ty.Callable[[], ty.Any]]
# receives the original in-process (func, args, kwargs) and returns a thunk
# producing the value to inject for that call.


class MemoUriCapture(Exception):
    """Raised by capturing_shim to surface the memo URI (and invocation metadata
    args) for a call, in place of executing it. Catch this to drive write_result
    imperatively."""

    def __init__(self, memo_uri: str, metadata_args: ty.Sequence[str]):
        super().__init__(memo_uri)
        self.memo_uri = memo_uri
        self.metadata_args = tuple(metadata_args)


def capturing_shim(shim_args: ty.Sequence[str]) -> None:
    """A shim that never computes: it raises MemoUriCapture carrying the memo URI.

    The orchestrator releases the invocation lock and propagates the exception.
    """
    raise MemoUriCapture(shim_args[1], shim_args[2:])


def write_result(memo_uri: str, value: ty.Any, *metadata_args: str) -> None:
    """Write `value` at `memo_uri` exactly as the remote entry would have written a
    computed result.

    metadata_args are the invocation-metadata CLI args a shim receives (everything
    after the memo URI); parse failures are a caller bug, not tolerated.
    """
    started_at = datetime.now(tz=timezone.utc)
    run_id = remote.generate_run_id(started_at)
    logger.info("Injecting precomputed result at %s (run_id %s)", memo_uri, run_id)
    with uris.ACTIVE_STORAGE_ROOT.set(uris.get_root(memo_uri)):
        route_return_value_or_exception(
            remote.ResultExcWithMetadataChannel(
                uris.lookup_blob_store(memo_uri),
                remote.result_dumper(),
                memo_uri,
                metadata.parse_invocation_metadata_args(metadata_args),
                started_at,
                run_id,
                extra_metadata={"precomputed_result": "true"},
            ),
            lambda: value,
            memo_uri,
            mprunner.RUNNER_NAME,
            invocation_run_id=run_id,
        )


def shim_builder(make_value: MakeValueThunk) -> ShimBuilder:
    """A ShimBuilder that injects precomputed results instead of computing.

    Every call reaching the shim (i.e. every memo miss) gets the value produced by
    `make_value(func, args, kwargs)` written as its result; the orchestrator then
    reads it back through the normal path, which also verifies it round-trips.

    Use via e.g. `with my_magic_func.shim(inject.shim_builder(make_value)): ...`
    """

    def build_injecting_shim(func: ty.Callable, args: Args, kwargs: Kwargs) -> SyncShim:
        thunk = make_value(func, args, kwargs)

        def inject_precomputed_result(shim_args: ty.Sequence[str]) -> None:
            write_result(shim_args[1], thunk(), *shim_args[2:])

        return inject_precomputed_result

    return build_injecting_shim
