"""Ask whether a memoized result exists - and get it - without ever computing one.

The contract: a peek produces exactly what calling the function would produce in
every case where the call would not compute, and returns `Unmemoized` in exactly the
cases where the call would compute. So a stored exception follows the runner's
`rerun_exceptions` setting (re-raised if a call would re-raise it, `Unmemoized` if a
call would recompute), and redirects and pipeline masks are honored because the memo
URI is derived by the same code a call uses.

A peek writes no invocation, result, exception, or lease state, and reports nothing:
a hit is not a cache hit *this* run made, so it emits no console event and no run
summary entry. Recording them would let polling invent history - N peeks would read
as N cache hits that never happened. The one thing a peek does write is `.shared()`
arguments, whose content-addressed bytes are uploaded inline while serializing (see
`pickling/sha256_b64.py`) - deriving the memo key requires serializing, and that
machinery has always written eagerly.
"""

import typing as ty

from thds.core import scope

from .._futures import MopsFuture
from ..core import memo
from ..core.types import Args, Kwargs
from . import prepare, types
from .get_results import read_value_or_raise

R = ty.TypeVar("R")


class UnmemoizedContextLost(Exception):
    """`Unmemoized.invoke()` was called where the call would use a different memo URI
    than the peek reported, so it would not fill the hole that was peeked."""


class Unmemoized(ty.Generic[R]):
    """No memoized result exists - calling the function would compute one.

    Process-local and ephemeral: a statement about the blob store at the moment of
    the peek. It deliberately cannot be pickled, so it can never be stored as (or
    inside) a memoized value or argument - serializing it would turn a fresh answer
    into a stale claim. Re-peek instead; it costs one existence check.

    `invoke()` makes the call the peek declined to make, filling exactly the
    `memo_uri` this handle reports. It carries the function memospace the peek
    resolved - blob root, pipeline id, memospace handlers and config, function name
    and logic keys - so it does not matter where it is finally called from: leaving
    the `pipeline_id_mask` the peek ran under does not move the hole.

    Only the *location* is fixed. How the call runs - the shim, and so the runtime it
    lands on - is resolved when `invoke()` is called, which is what lets a caller
    hold a handle and later run it wherever compute is actually available.

    The arguments hash is the one part of the location that cannot be carried,
    because `invoke()` must re-serialize to register the deferred uploads a peek
    deliberately dropped. So mutating an argument between the peek and the invoke
    would fill a different hole; `invoke()` re-derives the URI first and raises
    `UnmemoizedContextLost` before computing or writing anything.
    """

    __slots__ = ("memo_uri", "_invoke")

    def __init__(self, memo_uri: str, invoke: ty.Callable[[str], R]):
        self.memo_uri = memo_uri
        self._invoke = invoke

    def invoke(self) -> R:
        """The peeked call. Raises `UnmemoizedContextLost` if made somewhere that would
        not fill `memo_uri`; a result computed by anyone else in the meantime is
        returned rather than recomputed."""
        return self._invoke(self.memo_uri)

    def __reduce__(self) -> ty.NoReturn:
        raise TypeError(
            "Unmemoized cannot be serialized: it is a moment-in-time answer about the blob"
            " store, not a value. A mops-wrapped function must never return one, and"
            " code that wants the answer in another process should peek again there."
        )

    def __repr__(self) -> str:
        return f"Unmemoized({self.memo_uri})"


def invoke_filling(
    peeked_uri: str, derive_uri: ty.Callable[[], str], submit: ty.Callable[[], MopsFuture[R]]
) -> R:
    """Perform a peeked call, but only if it would fill `peeked_uri`.

    `derive_uri` re-derives the memo URI without invoking anything, so a call that
    would fill some other hole is refused before any work starts and before any
    invocation state is written. `submit` then performs it.
    """
    if (uri := derive_uri()) != peeked_uri:
        raise UnmemoizedContextLost(
            f"this Unmemoized reported {peeked_uri}, but invoking here would use"
            f" {uri} - so the call would not fill the hole that was peeked. Invoke it"
            " with the same arguments the peek was given; a mops-wrapped function must"
            " not be handed arguments that mutate underneath it."
        )
    return submit().result()


@scope.bound
def derive_memo_uri(
    serialize_args_kwargs: types.SerializeArgsKwargs,
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]],
    function_memospace: str,
    func: ty.Callable,
    args: Args,
    kwargs: Kwargs,
) -> str:
    """Where this call's result would live. Consults no stored result, and writes no
    invocation, result, exception, or lease state - though serializing the arguments
    still uploads any `.shared()` ones, as it does for a peek or a call. Deferred
    uploads registered while serializing are dropped with this scope."""
    return prepare.prepare_call(
        serialize_args_kwargs, calls_registry, function_memospace, func, args, kwargs
    ).memo_uri


@scope.bound
def peek_memoized(
    serialize_args_kwargs: types.SerializeArgsKwargs,
    get_meta_and_result: types.GetMetaAndResult,
    calls_registry: ty.Mapping[ty.Callable, ty.Collection[ty.Callable]],
    rerun_exceptions: bool,
    function_memospace: str,
    func: ty.Callable[..., R],
    args: Args,
    kwargs: Kwargs,
    invoke: ty.Callable[[str], R],
) -> ty.Union[R, Unmemoized[R]]:
    """The runner-generic peek: derive the memo URI exactly as an invocation would,
    then read the store or report `Unmemoized`.

    `invoke` performs the declined call, and receives the memo URI this peek derived
    so it can refuse to fill any other one (see `invoke_filling`).

    Argument serialization may register deferred uploads (e.g. for Source
    arguments); they are dropped when this scope exits, which is safe because they
    are only ever needed by an invocation. Shared-argument bytes are not deferred and
    are written - see this module's docstring."""
    prepared = prepare.prepare_call(
        serialize_args_kwargs, calls_registry, function_memospace, func, args, kwargs
    )
    result = memo.results.read_result(prepared.memo_uri, check_for_exception=not rerun_exceptions)
    if not result:
        return Unmemoized(prepared.memo_uri, invoke)

    return read_value_or_raise(get_meta_and_result, result)
