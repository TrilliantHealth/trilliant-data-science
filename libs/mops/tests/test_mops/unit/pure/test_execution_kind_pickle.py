import pickle

import pytest

from thds.core import futures
from thds.mops.pure import ExecutionKind, MopsFuture
from thds.mops.pure.core import metadata
from thds.mops.pure.runner import lease_waiter


def _completed_tuple() -> futures.PFuture[tuple[int, metadata.ResultMetadata | None]]:
    return futures.resolved((42, None))


@pytest.mark.parametrize("kind", ["computed", "shared"])
def test_awaited_execution_kind_survives_multiple_pickle_hops(kind: ExecutionKind) -> None:
    awaited: lease_waiter._AwaitedFuture[int] = lease_waiter._AwaitedFuture("memo://example")
    awaited.execution_kind = kind
    if kind == "computed":
        awaited._takeover_future = MopsFuture.from_tuple_future(
            futures.LazyFuture(_completed_tuple), "memo://example", "computed"
        )
    else:
        awaited.set_result((42, None))
    future = MopsFuture.from_tuple_future(awaited, "memo://example")
    for _ in range(3):
        future = pickle.loads(pickle.dumps(future))
        assert future.execution_kind == kind
        assert future.result() == 42
