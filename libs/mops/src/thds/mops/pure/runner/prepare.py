"""The 'prepare' phase shared by invoking and peeking: everything that must happen
to know a call's memo URI, and nothing that writes to the blob store.
"""

import typing as ty

from thds.core import concurrency, scope

from ..._utils.on_slow import LogSlow, on_slow
from ...config import max_concurrent_serialization
from ..core import deferred_work, memo, pipeline_id_mask, uris
from ..core.partial import unwrap_partial
from ..core.types import Args, BlobStore, Kwargs
from . import types

# Pickling is 100% GIL-bound — hundreds of threads pickling simultaneously
# just convoy behind the GIL, inflating wall-clock time per thread without
# improving throughput. A semaphore limits concurrency so each pickle finishes fast.
# Reentrant because serialization (__getstate__) can trigger lazy mops calls
# that themselves need to serialize — blocking the same thread would deadlock.
_SERIALIZATION_SEMAPHORE = concurrency.ReentrantBoundedSemaphore(int(max_concurrent_serialization()))


class PreparedCall(ty.NamedTuple):
    """A call whose memo URI is fully derived. `func`, `args`, and `kwargs` are the
    partial-unwrapped forms actually hashed - not necessarily what the caller passed."""

    storage_root: str
    fs: BlobStore
    func: ty.Callable
    args: Args
    kwargs: Kwargs
    pipeline_id: str
    args_kwargs_bytes: bytes
    memo_uri: str


def prepare_call(
    serialize_args_kwargs: types.SerializeArgsKwargs,
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]],
    function_memospace: str,
    func: ty.Callable,
    args_: Args,
    kwargs_: Kwargs,
) -> PreparedCall:
    """Serialize the arguments and derive the memo URI.

    Writes no invocation, result, exception, or lease state. It is not write-free,
    though: serializing a `.shared()` argument uploads its content-addressed bytes
    inline (see `pickling/sha256_b64.py`), which is why a peek is not either.

    Enters stack-scoped contexts (active storage root, pipeline-id mask, deferred
    work) that must outlive the return, so the caller must be `@scope.bound`; any
    deferred uploads registered during serialization are the caller's to perform
    (or to drop, for a read-only caller like peek)."""
    storage_root = uris.get_root(function_memospace)
    scope.enter(uris.ACTIVE_STORAGE_ROOT.set(storage_root))
    fs = uris.lookup_blob_store(function_memospace)

    # we need to unwrap any partial object and combine its wrapped
    # args, kwargs with the provided args, kwargs, otherwise the
    # args and kwargs will not get properly considered in the memoization key.
    func, args, kwargs = unwrap_partial(func, args_, kwargs_)
    pipeline_id = scope.enter(pipeline_id_mask.including_function_docstr(func))
    # TODO pipeline_id should probably be passed in explicitly

    scope.enter(deferred_work.open_context())  # optimize Source objects during serialization

    with (
        _SERIALIZATION_SEMAPHORE,
        on_slow(lambda s: LogSlow(f"serialize_args_kwargs took {s:.1f}s for {function_memospace}")),
    ):
        args_kwargs_bytes = serialize_args_kwargs(storage_root, func, args, kwargs)
    memo_uri = fs.join(
        function_memospace,
        *memo.calls.combine_function_logic_keys(memo.calls.resolve(calls_registry, func)),
        # ^ these will embedded as extra nesting.
        memo.args_kwargs_content_address(args_kwargs_bytes),
    )
    return PreparedCall(storage_root, fs, func, args, kwargs, pipeline_id, args_kwargs_bytes, memo_uri)
