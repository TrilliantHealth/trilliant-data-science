import datetime as dt
import threading
import time

import pytest

from thds.mops.pure.core import file_blob_store
from thds.mops.pure.tools.console import (
    describer,
    enabled,
    refusals,
    run_metadata,
    run_name,
    throwaway,
    upload,
    writer,
)

_MEMO_URI_SUFFIX = "mops2-mpf/pipe/pkg.mod--fn/hash123"
_RUN = "2026-09-30/mr.Run.abc"


@pytest.fixture(autouse=True)
def _reset(monkeypatch, tmp_path):
    monkeypatch.setattr(throwaway, "here", lambda: False)
    upload._reset()
    run_metadata._reset_for_test()
    describer._reset_for_test()
    refusals._reset_for_test()
    with (
        run_name.RUN_NAME.set_local(_RUN),
        writer.CONSOLE_EVENTS_DIR.set_local(tmp_path / "local-events"),
    ):
        yield
    if isinstance(writer._WRITER, writer._Writer):
        writer._WRITER.close()
    writer._WRITER = None
    upload._reset()
    run_metadata._reset_for_test()
    describer._reset_for_test()
    refusals._reset_for_test()


def _wrap_putbytes(monkeypatch, on_put):
    real = file_blob_store.FileBlobStore.putbytes

    def putbytes(self, remote_uri, data, type_hint="bytes"):
        on_put(remote_uri)
        return real(self, remote_uri, data, type_hint=type_hint)

    monkeypatch.setattr(file_blob_store.FileBlobStore, "putbytes", putbytes)


def _eventually(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def test_an_invocation_does_not_wait_on_describing_the_run(tmp_path, monkeypatch):
    """A blob store that takes minutes to answer (or to give up retrying) delays the
    run's description, never the invocation that prompted it."""
    release = threading.Event()
    _wrap_putbytes(monkeypatch, lambda uri: release.wait(10))

    started = time.monotonic()
    enabled.invoked(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", attempt_id="w1", at=dt.datetime.now())
    assert time.monotonic() - started < 1.0

    release.set()
    assert _eventually(lambda: bool(list((tmp_path / "mops/console").rglob("*.toml"))))


def test_a_refused_root_is_written_to_once(tmp_path, monkeypatch, caplog):
    """The first refusal drops the root for the rest of the process: its description,
    event batches and manifests are all skipped, and one warning says so."""
    attempts: list[str] = []

    def refuse(uri):
        attempts.append(uri)
        raise PermissionError(f"not yours to write: {uri}")

    _wrap_putbytes(monkeypatch, refuse)
    memo_uri = f"file://{tmp_path}/{_MEMO_URI_SUFFIX}"
    root = f"file://{tmp_path}/mops/console/{_RUN}"

    for _ in range(3):
        upload.start(memo_uri, _RUN)
        assert _eventually(lambda: refusals.refused(root))
        upload.add_to(root, [{"event": "invoked", "at": "2026-09-30T12:00:00+00:00"}])
        upload.flush()

    assert len(attempts) == 1
    assert [r.levelname for r in caplog.records if "refused" in r.getMessage()] == ["WARNING"]


def test_a_refused_root_leaves_the_others_publishing(tmp_path, monkeypatch):
    refused_dir = tmp_path / "refused"
    open_dir = tmp_path / "open"

    def refuse(uri):
        if str(refused_dir) in uri:
            raise PermissionError(uri)

    _wrap_putbytes(monkeypatch, refuse)
    upload.start(f"file://{refused_dir}/{_MEMO_URI_SUFFIX}", _RUN)
    upload.start(f"file://{open_dir}/{_MEMO_URI_SUFFIX}", _RUN)

    assert _eventually(lambda: bool(list((open_dir / "mops/console").rglob("*.toml"))))
    assert _eventually(lambda: refusals.refused(f"file://{refused_dir}/mops/console/{_RUN}"))
    upload.flush()
    assert list((open_dir / "mops/console").rglob("roots/*.json"))


def test_a_slow_root_does_not_hold_up_describing_another(tmp_path, monkeypatch):
    slow_dir = tmp_path / "slow"
    fast_dir = tmp_path / "fast"
    release = threading.Event()

    def stall(uri):
        if str(slow_dir) in uri:
            release.wait(10)

    _wrap_putbytes(monkeypatch, stall)
    upload.start(f"file://{slow_dir}/{_MEMO_URI_SUFFIX}", _RUN)
    upload.start(f"file://{fast_dir}/{_MEMO_URI_SUFFIX}", _RUN)

    assert _eventually(lambda: bool(list((fast_dir / "mops/console").rglob("*.toml"))))
    assert _eventually(lambda: bool(list((fast_dir / "mops/console").rglob("_index/*"))))
    assert not list((slow_dir / "mops/console").rglob("*.toml"))
    release.set()
