import json

import pytest
import tomli

from thds.mops.pure.tools.console import run_metadata, throwaway, upload

_MEMO_URI_SUFFIX = "mops2-mpf/pipe/pkg.mod--fn/hash123"


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(throwaway, "here", lambda: False)
    # these assert against `mops/console` paths, and running under pytest would otherwise
    # send them to the throwaway location. Where that redirect happens is tested in
    # `test_console_blob_sink`; what is under test here is the batching.
    upload._reset()
    run_metadata._reset_for_test()
    yield
    upload._reset()
    run_metadata._reset_for_test()


def _event(key, at="2026-08-07T12:00:00+00:00"):
    return {"event": "invoked", "invocation_key": key, "at": at, "function_name": "m:f"}


def _add_to_only_root(events):
    [root] = upload.roots()
    upload.add_to(root, events)


def test_nothing_is_published_before_a_run_is_named(tmp_path):
    """A run with the console off mints no name, and must not write to the blob store."""
    upload.add_to("file:///run-not-started", [_event("a")])
    upload.flush()

    assert not list(tmp_path.rglob("*.json"))


def test_a_batch_becomes_one_object(tmp_path):
    """The whole point: an orchestrator dispatching tens of thousands of invocations must
    not write an object each."""
    upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    _add_to_only_root([_event("a"), _event("b"), _event("c")])
    upload.flush()

    written = list((tmp_path / "mops/console/mr.Run.abc/events").iterdir())
    assert len(written) == 1
    assert len(written[0].read_text().splitlines()) == 3


def test_start_publishes_run_metadata_immediately(tmp_path):
    upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")

    run_root = tmp_path / "mops/console/mr.Run.abc"
    metadata_files = list(run_root.glob("*.toml"))
    assert len(metadata_files) == 1
    assert tomli.loads(metadata_files[0].read_text())["run_name"] == "mr.Run.abc"
    assert not (run_root / "events").exists()


def test_flushing_twice_does_not_republish(tmp_path):
    upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    _add_to_only_root([_event("a")])
    upload.flush()
    upload.flush()

    assert len(list((tmp_path / "mops/console/mr.Run.abc/events").iterdir())) == 1


def test_an_unwritable_destination_does_not_raise(tmp_path):
    """Publishing is diagnostic. Failing it must never interrupt the run it describes."""
    upload.start("not-a-uri-at-all", "mr.Run.abc")
    upload.add_to("not-a-uri-at-all", [_event("a")])

    upload.flush()  # must not raise


def test_starting_twice_on_the_same_root_keeps_the_first(tmp_path):
    """Every invocation calls start; a root seen before is not re-created."""
    upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.First.abc")
    upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.First.abc")
    _add_to_only_root([_event("a")])
    upload.flush()

    assert len(list((tmp_path / "mops/console/mr.First.abc/events").iterdir())) == 1


def test_publishing_can_be_turned_off(tmp_path):
    with upload.CONSOLE_UPLOAD_EVENTS.set_local(False):
        upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
        upload.add_to("file:///upload-disabled", [_event("a")])
        upload.flush()

    assert not list(tmp_path.rglob("*.json"))


def test_two_roots_get_separate_uploaders(tmp_path):
    """A run spanning blob roots publishes to each independently."""
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    upload.start(f"file://{root_a}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    upload.start(f"file://{root_b}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")

    roots = sorted(upload.roots())
    upload.add_to(roots[0], [{**_event("x"), "memo_uri": f"file://{root_a}/{_MEMO_URI_SUFFIX}"}])
    upload.add_to(roots[1], [{**_event("y"), "memo_uri": f"file://{root_b}/{_MEMO_URI_SUFFIX}"}])
    upload.flush()

    a_events = list((root_a / "mops/console/mr.Run.abc/events").iterdir())
    b_events = list((root_b / "mops/console/mr.Run.abc/events").iterdir())
    assert len(a_events) == 1
    assert len(b_events) == 1
    assert json.loads(a_events[0].read_text())["invocation_key"] == "x"
    assert json.loads(b_events[0].read_text())["invocation_key"] == "y"


def _latest_manifest(roots_dir):
    """The manifest file with the highest root count (last written)."""
    return max(roots_dir.glob("*.json"), key=lambda p: int(p.stem.rsplit("-", 1)[-1]))


def test_a_manifest_is_published_with_all_roots(tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    upload.start(f"file://{root_a}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    upload.start(f"file://{root_b}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    upload.add_to(
        sorted(upload.roots())[0],
        [{**_event("x"), "memo_uri": f"file://{root_a}/{_MEMO_URI_SUFFIX}"}],
    )
    upload.flush()

    manifest_a = json.loads(_latest_manifest(root_a / "mops/console/mr.Run.abc/roots").read_text())
    manifest_b = json.loads(_latest_manifest(root_b / "mops/console/mr.Run.abc/roots").read_text())
    assert len(manifest_a["roots"]) == 2
    assert manifest_a == manifest_b


def test_a_failed_manifest_write_is_retried(tmp_path):
    """A transient failure on one root must not prevent retry on the next flush."""
    from unittest import mock

    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    upload.start(f"file://{root_a}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    upload.start(f"file://{root_b}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    upload.add_to(
        sorted(upload.roots())[0],
        [{**_event("x"), "memo_uri": f"file://{root_a}/{_MEMO_URI_SUFFIX}"}],
    )

    roots_a = root_a / "mops/console/mr.Run.abc/roots"
    roots_b = root_b / "mops/console/mr.Run.abc/roots"
    original_putbytes = upload.uris.lookup_blob_store(f"file://{root_a}").putbytes

    def fail_on_a(uri, *args, **kwargs):
        if str(root_a) in uri and "/roots/" in uri:
            raise OSError("transient")

        return original_putbytes(uri, *args, **kwargs)

    with mock.patch.object(
        upload.uris.lookup_blob_store(f"file://{root_a}"), "putbytes", side_effect=fail_on_a
    ):
        upload.flush()

    assert list(roots_b.glob("*.json"))
    assert not roots_a.exists() or not list(roots_a.glob("*.json"))

    upload.flush()
    assert list(roots_a.glob("*.json"))
    manifest_a = json.loads(_latest_manifest(roots_a).read_text())
    manifest_b = json.loads(_latest_manifest(roots_b).read_text())
    assert manifest_a == manifest_b


def test_batches_carry_every_event_verbatim(tmp_path):
    """No summarising on the way out - a second observer must be able to reconstruct the
    same state a local reader would."""
    upload.start(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    _add_to_only_root([_event("a"), _event("b")])
    upload.flush()

    written = next(iter((tmp_path / "mops/console/mr.Run.abc/events").iterdir()))
    assert [json.loads(line)["invocation_key"] for line in written.read_text().splitlines()] == [
        "a",
        "b",
    ]


def test_a_manifest_is_backfilled_under_roots_this_process_never_wrote_to(tmp_path):
    """Workers of one run may each write to a different blob root and share none; what
    they do share is the run's local pointer file. Its contents are published as a
    manifest under every root - including one whose own writer has already exited - so
    a remote watcher entering any single root discovers the rest."""
    root_a = tmp_path / "a"
    root_a.mkdir()
    upload.start(f"file://{root_a}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    _add_to_only_root([{**_event("x"), "memo_uri": f"file://{root_a}/{_MEMO_URI_SUFFIX}"}])

    elsewhere = f"file://{tmp_path}/b/mops/console/mr.Run.abc"
    upload.flush(known_roots=(elsewhere,))

    own = json.loads(_latest_manifest(root_a / "mops/console/mr.Run.abc/roots").read_text())
    assert elsewhere in own["roots"]
    assert len(own["roots"]) == 2
    backfilled = json.loads(_latest_manifest(tmp_path / "b/mops/console/mr.Run.abc/roots").read_text())
    assert backfilled == own


def test_an_unwritable_known_root_does_not_raise(tmp_path):
    """Nothing guarantees write access to a root some other process chose. A refused
    backfill is a debug line, never an error - and it must not block the manifest under
    this process's own root."""
    root_a = tmp_path / "a"
    root_a.mkdir()
    upload.start(f"file://{root_a}/{_MEMO_URI_SUFFIX}", "mr.Run.abc")
    _add_to_only_root([{**_event("x"), "memo_uri": f"file://{root_a}/{_MEMO_URI_SUFFIX}"}])

    upload.flush(known_roots=("not-a-uri-at-all",))  # must not raise

    manifest = json.loads(_latest_manifest(root_a / "mops/console/mr.Run.abc/roots").read_text())
    assert "not-a-uri-at-all" in manifest["roots"]
