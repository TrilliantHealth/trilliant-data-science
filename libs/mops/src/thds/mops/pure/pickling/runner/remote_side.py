import typing as ty

from thds.core import scope

from ...._utils.once import Once
from ....srcdest.mark_remote import mark_as_remote
from ...core.entry import register_entry_handler, route_result_or_exception
from ...core.pipeline_id_mask import pipeline_id_mask
from ...core.serialize_big_objs import ByIdRegistry, ByIdSerializer
from ...core.serialize_paths import CoordinatingPathSerializer
from ...core.types import BlobStore, T
from ...core.uris import get_root, lookup_blob_store
from .._pickle import Dumper, gimme_bytes, make_read_object, unfreeze_args_kwargs
from ..pickles import NestedFunctionPickle
from . import sha256_b64
from .orchestrator_side import EXCEPTION, INVOCATION, RESULT, RUNNER_SUFFIX, MemoizingPicklingRunner


class _ResultExcChannel(ty.NamedTuple):
    fs: BlobStore
    dumper: Dumper
    call_id: str

    def result(self, r: T):
        self.fs.putbytes(
            self.fs.join(self.call_id, RESULT),
            gimme_bytes(self.dumper, r),
            type_hint=RESULT,
        )

    def exception(self, exc: Exception):
        self.fs.putbytes(
            self.fs.join(self.call_id, EXCEPTION),
            gimme_bytes(self.dumper, exc),
            type_hint="EXCEPTION",
        )


def remote_entry_run_pickled_invocation(memo_uri: str, pipeline_id: str):
    """The arguments are those supplied by MemoizingPicklingFunctionRunner."""
    fs = lookup_blob_store(memo_uri)

    def do_work_return_result() -> object:
        nested = ty.cast(
            NestedFunctionPickle,
            make_read_object(INVOCATION)(fs.join(memo_uri, INVOCATION)),
        )
        args, kwargs = mark_as_remote(unfreeze_args_kwargs(nested.args_kwargs_pickle))
        return pipeline_id_mask(pipeline_id)(nested.f)(*args, **kwargs)

    def _extract_invocation_unique_key(memo_uri: str) -> ty.Tuple[str, str]:
        parts = fs.split(memo_uri)
        runner_idx = parts.index(RUNNER_SUFFIX)
        assert runner_idx >= 0, f"This URI does not look like it was created by us: {memo_uri}"
        invocation_parts = parts[runner_idx + 1 :]
        return fs.join(*invocation_parts[:-1]), invocation_parts[-1]

    scope.enter(sha256_b64.DEFERRED_STORAGE_ROOT.set(get_root(memo_uri)))
    route_result_or_exception(
        _ResultExcChannel(
            fs,
            Dumper(
                ByIdSerializer(ByIdRegistry()),
                CoordinatingPathSerializer(sha256_b64.Sha256B64PathStream(), Once()),
            ),
            memo_uri,
        ),
        ty.cast(ty.Callable[[], T], do_work_return_result),
        pipeline_id,
        _extract_invocation_unique_key(memo_uri),
    )


register_entry_handler(
    MemoizingPicklingRunner.__name__,
    remote_entry_run_pickled_invocation,  # type: ignore
)
