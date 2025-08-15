import contextlib
import typing as ty
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property

from thds.core import log, scope

from ..._utils.once import Once
from ..core import deferred_work, lock, metadata, pipeline_id, uris
from ..core.entry import route_return_value_or_exception
from ..core.memo import results
from ..core.serialize_big_objs import ByIdRegistry, ByIdSerializer
from ..core.serialize_paths import CoordinatingPathSerializer
from ..core.types import Args, BlobStore, Kwargs, T
from ..core.use_runner import unwrap_use_runner
from ..runner import strings
from . import _pickle, mprunner, pickles, sha256_b64

logger = log.getLogger(__name__)


@dataclass  # needed for cached_property
class _ResultExcWithMetadataChannel:
    fs: BlobStore
    dumper: _pickle.Dumper
    call_id: str
    invocation_metadata: metadata.InvocationMetadata
    started_at: datetime

    @cached_property
    def _metadata_header(self) -> bytes:
        """This is always embedded _alongside_ the actual return value or exception.
        This is to make sure that whatever metadata is in the result is atomically
        part of the result, such that in the rare case of racing invocations,
        the metadata can be trusted to be accurate.
        """
        result_metadata = metadata.ResultMetadata.from_invocation(
            self.invocation_metadata, self.started_at, datetime.now(tz=timezone.utc)
        )
        logger.info(f"Remote code version: {result_metadata.remote_code_version}")
        return metadata.format_result_header(result_metadata).encode("utf-8")

    def _write_metadata_only(self, prefix: str) -> None:
        """This is a mops v3 thing that is unnecessary but adds clarity when debugging.
        If you see more than one of these files in a directory, that usually means either
        the success was preceded by a failure, _or_ it means that there was an (unusual) race condition.
        """
        self.fs.putbytes(
            self.fs.join(self.call_id, f"{prefix}-metadata-{self.invocation_metadata.invoker_uuid}.txt"),
            self._metadata_header,
            type_hint="text/plain",
        )

    def return_value(self, r: T) -> None:
        result_uri = self.fs.join(self.call_id, results.RESULT)
        if self.fs.exists(result_uri):
            logger.warning("Not overwriting existing result at %s prior to serialization", result_uri)
            self._write_metadata_only("lost-race")
            return

        # when we pickle the return value, we also end up potentially uploading
        # various Sources and Paths and other special-cased things inside the result.
        return_value_bytes = _pickle.gimme_bytes(self.dumper, r)
        if self.fs.exists(result_uri):
            logger.warning("Not overwriting existing result at %s after serialization", result_uri)
            self._write_metadata_only("lost-race-after-serialization")
            return

        # It's important that all deferred work is performed before the return
        # value is written to the blob store so that result consumers don't read
        # inconsistent data. For example, one type of deferred work is uploading
        # result sources. If the invocation result is written with the source
        # uri before the source is uploaded, then result consumers might try to
        # download a non-existent file in the meantime.
        deferred_work.perform_all()

        # BUG: there remains a race condition between fs.exists and putbytes.
        # multiple callers could get a False from fs.exists and then proceed to write.
        # the biggest issue here is for functions that are not truly pure, because
        # they will be writing different results, and theoretically different callers
        # could end up seeing the different results.
        #
        # In the future, if a Blob Store provided a put_unless_exists method, we could use
        # that to avoid the race condition.
        self.fs.putbytes(
            result_uri,
            self._metadata_header + return_value_bytes,
            type_hint="application/mops-return-value",
        )
        self._write_metadata_only("result")

    def exception(self, exc: Exception) -> None:
        exc_bytes = _pickle.gimme_bytes(self.dumper, exc)
        self.fs.putbytes(
            self.fs.join(self.call_id, results.EXCEPTION),
            self._metadata_header + exc_bytes,
            type_hint="application/mops-exception",
        )
        self._write_metadata_only("exception")


def _unpickle_invocation(memo_uri: str) -> ty.Tuple[ty.Callable, Args, Kwargs]:
    _, invocation_raw = _pickle.make_read_header_and_object(strings.INVOCATION)(
        uris.lookup_blob_store(memo_uri).join(memo_uri, strings.INVOCATION)
    )
    invocation = ty.cast(pickles.Invocation, invocation_raw)
    args, kwargs = _pickle.unfreeze_args_kwargs(invocation.args_kwargs_pickle)
    return invocation.func, args, kwargs


@contextlib.contextmanager
def _manage_lock(lock_uri: str, lock_writer_id: str) -> ty.Iterator[ty.Optional[Exception]]:
    stop_lock: ty.Callable = lambda: None  # noqa: E731
    try:
        stop_lock = lock.add_lock_to_maintenance_daemon(
            lock.make_remote_lock_writer(lock_uri, expected_writer_id=lock_writer_id)
        )
        yield None  # pause for execution
    except lock.CannotMaintainLock as e:
        logger.info(f"Cannot maintain lock: {e}. Continuing without the lock.")
        yield None  # pause for execution
    except lock.LockWasStolenError as stolen_lock_error:
        logger.error(f"Lock was stolen: {stolen_lock_error}. Will exit without running the function.")
        yield stolen_lock_error  # pause to upload failure

    stop_lock()  # not critical since we don't _own_ the lock, but keeps things cleaner


@scope.bound
def run_pickled_invocation(memo_uri: str, *metadata_args: str) -> None:
    """The arguments are those supplied by MemoizingPicklingRunner.

    As of v3, we now expect a number of (required) metadata arguments with every invocation.
    """
    started_at = datetime.now(tz=timezone.utc)  # capture this timestamp right at the outset.
    invocation_metadata = metadata.parse_invocation_metadata_args(metadata_args)
    metadata.INVOKED_BY.set_global(invocation_metadata.invoked_by)
    pipeline_id.set_pipeline_id(invocation_metadata.pipeline_id)
    fs = uris.lookup_blob_store(memo_uri)

    # any recursively-called functions that use metadata will retain the original invoker.

    def _extract_invocation_unique_key(memo_uri: str) -> ty.Tuple[str, str]:
        parts = fs.split(memo_uri)
        try:
            runner_idx = parts.index(mprunner.RUNNER_NAME)
        except ValueError as ve:
            raise ValueError(
                f"Unable to find the runner name {mprunner.RUNNER_NAME} in parts {parts}"
            ) from ve
        invocation_parts = parts[runner_idx + 1 :]
        return fs.join(*invocation_parts[:-1]), invocation_parts[-1]

    lock_error = scope.enter(_manage_lock(fs.join(memo_uri, "lock"), invocation_metadata.invoker_uuid))
    scope.enter(uris.ACTIVE_STORAGE_ROOT.set(uris.get_root(memo_uri)))

    try:
        func, args, kwargs = _unpickle_invocation(memo_uri)
    except Exception:
        logger.error(f"Failed to unpickle invocation from {memo_uri} - this is a bug in mops!")
        raise

    def do_work_return_result() -> object:
        # ONLY failures in this code should transmit an EXCEPTION
        # back to the orchestrator side.

        # if the lock was stolen, we will write an exception
        # so that the orchestrator knows that it failed.
        # in theory, it could resume waiting for a result, though
        # currently it does not do this.
        if lock_error:
            raise lock_error

        with unwrap_use_runner(func):
            return func(*args, **kwargs)

    route_return_value_or_exception(
        _ResultExcWithMetadataChannel(
            fs,
            _pickle.Dumper(
                ByIdSerializer(ByIdRegistry()),
                CoordinatingPathSerializer(sha256_b64.Sha256B64PathStream(), Once()),
                _pickle.SourceResultPickler(),
            ),
            memo_uri,
            invocation_metadata,
            started_at,
        ),
        ty.cast(ty.Callable[[], T], do_work_return_result),
        invocation_metadata.pipeline_id,
        _extract_invocation_unique_key(memo_uri),
    )
