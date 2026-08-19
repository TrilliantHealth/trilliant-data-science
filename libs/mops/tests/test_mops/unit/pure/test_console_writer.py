import datetime as dt
import json
import os
from pathlib import Path

import pytest

from thds.mops.pure.tools import console
from thds.mops.pure.tools.console import events, run_metadata, run_name, throwaway, upload, writer

_AT = dt.datetime(2026, 8, 6, 12, 0, tzinfo=dt.timezone.utc)


@pytest.fixture(autouse=True)
def _real_run(monkeypatch):
    """These assert where a real run's events go, and run under pytest - which is one of
    the things that moves them aside. That redirect is tested in `test_console_throwaway`."""
    monkeypatch.setattr(throwaway, "here", lambda: False)
    upload._reset()
    run_metadata._reset_for_test()
    yield
    _reset_writer()
    upload._reset()
    run_metadata._reset_for_test()


def test_invoked_decomposes_the_memo_uri():
    event = console.events.invoked(
        "adls://sa/container/mops2-mpf/my-pipeline/my_pkg.mod--run/abc123",
        attempt_id="Writer.xyz",
        at=_AT,
    )

    assert event["invocation_key"] == "my-pipeline/my_pkg.mod:run/abc123"
    assert event["pipeline_id"] == "my-pipeline"
    assert event["function_name"] == "my_pkg.mod:run"
    assert event["attempt_id"] == "Writer.xyz"


def test_invoked_falls_back_to_the_raw_uri_when_unparseable():
    event = console.events.invoked("not-a-memo-uri", attempt_id="x", at=_AT)

    assert event["invocation_key"] == "not-a-memo-uri"
    assert event["pipeline_id"] == ""


def test_parsed_key_is_storage_root_independent():
    """The same invocation under two blob roots must produce one key - that is what lets
    a reader correlate work across machines."""
    suffix = "mops2-mpf/pipe/pkg.mod--fn/hash123"

    assert (
        events._parse(f"adls://sa/container/{suffix}").invocation_key
        == events._parse(f"file:///tmp/local/{suffix}").invocation_key
    )


def _events_in(events_dir: Path) -> list[dict]:
    return [
        json.loads(line)
        for path in events_dir.glob("*.jsonl")
        for line in path.read_text().splitlines()
        if line
    ]


def _reset_writer() -> None:
    if isinstance(writer._WRITER, writer._Writer):
        writer._WRITER.close()
    writer._WRITER = None


def test_an_empty_directory_setting_disables_writing(tmp_path):
    """The kill switch: events are on by default, and this is how a run opts out."""
    _reset_writer()
    with writer.CONSOLE_EVENTS_DIR.set_local(Path("")):
        console.emit({"event": "invoked", "invocation_key": "a/b/c"})

    assert writer._WRITER is None
    assert not list(tmp_path.iterdir())


def test_events_are_filed_under_the_run_not_the_process(tmp_path):
    """One directory per run, whatever process is writing.

    The summary tree's run name carries the writing process's pid, so using it here would
    scatter a pool-dispatched run across one directory per worker.
    """
    _reset_writer()
    with writer.CONSOLE_EVENTS_DIR.set_local(tmp_path / "events"):
        with run_name.RUN_NAME.set_local("2026-08-09/mr.Named"):
            assert writer.events_dir() == tmp_path / "events" / "2026-08-09" / "mr.Named"


def test_emits_one_line_per_event(tmp_path):
    _reset_writer()
    events_dir = tmp_path / "events"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir):
        console.emit({"event": "invoked", "invocation_key": "a/b/c"})
        console.emit({"event": "completed", "invocation_key": "a/b/c"})
        _reset_writer()  # closes and flushes

        assert [e["event"] for e in _events_in(writer.events_dir())] == ["invoked", "completed"]


def test_file_is_named_for_the_writing_process(tmp_path):
    _reset_writer()
    events_dir = tmp_path / "events"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir):
        console.emit({"event": "invoked", "invocation_key": "a/b/c"})
        _reset_writer()

        assert [p.name for p in writer.events_dir().glob("*.jsonl")] == [f"events-{os.getpid()}.jsonl"]


def _memo_uri(root: Path, args_hash: str) -> str:
    return f"file://{root}/mops2-mpf/pipe/pkg.mod--fn/{args_hash}"


def test_a_memoized_only_run_stays_local(tmp_path):
    events_dir = tmp_path / "events"
    blob_root = tmp_path / "blob"
    run = "2026-08-19/mr.MemoOnly.abc"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir), run_name.RUN_NAME.set_local(run):
        run_name.claim()
        console.memoized(_memo_uri(blob_root, "cached"), at=_AT)
        _reset_writer()

        assert [event["was_memoized"] for event in _events_in(writer.events_dir())] == [True]
        assert not (blob_root / "mops/console" / run).exists()


def test_the_first_invocation_publishes_the_local_history_before_it(tmp_path):
    events_dir = tmp_path / "events"
    blob_root = tmp_path / "blob"
    run = "2026-08-19/mr.Partial.abc"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir), run_name.RUN_NAME.set_local(run):
        console.memoized(_memo_uri(blob_root, "cached"), at=_AT)
        console.invoked(_memo_uri(blob_root, "new"), attempt_id="Writer.new", at=_AT)
        _reset_writer()

        published = [
            json.loads(line)
            for path in (blob_root / "mops/console" / run / "events").iterdir()
            for line in path.read_text().splitlines()
        ]
        assert [(event["event"], event.get("was_memoized", False)) for event in published] == [
            ("completed", True),
            ("invoked", False),
        ]


def test_an_invocation_publishes_cache_hits_from_other_roots(tmp_path):
    events_dir = tmp_path / "events"
    cached_root = tmp_path / "cached"
    invoked_root = tmp_path / "invoked"
    run = "2026-08-19/mr.ManyRoots.abc"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir), run_name.RUN_NAME.set_local(run):
        console.memoized(_memo_uri(cached_root, "cached"), at=_AT)
        console.invoked(_memo_uri(invoked_root, "new"), attempt_id="Writer.new", at=_AT)
        _reset_writer()

        def published(root: Path) -> list[dict]:
            return [
                json.loads(line)
                for path in (root / "mops/console" / run / "events").iterdir()
                for line in path.read_text().splitlines()
            ]

        assert [event["invocation_key"] for event in published(cached_root)] == [
            "pipe/pkg.mod:fn/cached"
        ]
        assert [event["invocation_key"] for event in published(invoked_root)] == ["pipe/pkg.mod:fn/new"]


def test_an_invocation_in_one_writer_publishes_another_writers_cache_hits(tmp_path):
    blob_root = tmp_path / "blob"
    run = "2026-08-19/mr.ManyProcesses.abc"
    with writer.CONSOLE_EVENTS_DIR.set_local(tmp_path / "events"), run_name.RUN_NAME.set_local(run):
        directory = writer.events_dir()
        directory.mkdir(parents=True)
        memo_writer = writer._Writer(directory / "events-memo-process.jsonl", run)
        invoking_writer = writer._Writer(directory / "events-invoking-process.jsonl", run)
        memo_uri = _memo_uri(blob_root, "cached")
        invoked_uri = _memo_uri(blob_root, "new")

        memo_writer.emit(events.memoized(memo_uri, at=_AT))
        upload.start(invoked_uri, run)
        writer.record_remote_events_uri(upload.roots()[0])
        invoking_writer.emit(events.invoked(invoked_uri, attempt_id="Writer.new", at=_AT))
        memo_writer.close()
        invoking_writer.close()

        published = [
            json.loads(line)
            for path in (blob_root / "mops/console" / run / "events").iterdir()
            for line in path.read_text().splitlines()
        ]
        assert {event["invocation_key"] for event in published} == {
            "pipe/pkg.mod:fn/cached",
            "pipe/pkg.mod:fn/new",
        }


def test_an_unusable_directory_is_disabled_after_one_attempt(tmp_path, monkeypatch):
    """A broken events dir must not be retried once per emit - on a hot path that would
    mean tens of thousands of failed syscalls and tracebacks."""
    _reset_writer()
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")

    attempts = 0
    real_mkdir = Path.mkdir

    def counting_mkdir(self, *a, **kw):
        nonlocal attempts
        attempts += 1
        return real_mkdir(self, *a, **kw)

    monkeypatch.setattr(Path, "mkdir", counting_mkdir)
    with writer.CONSOLE_EVENTS_DIR.set_local(blocker):
        for _ in range(50):
            console.emit({"event": "invoked", "invocation_key": "a/b/c"})

        assert attempts == 1
        assert writer._writer() is None


def test_full_queue_drops_rather_than_blocking(tmp_path, monkeypatch):
    _reset_writer()
    monkeypatch.setattr(writer, "_MAX_QUEUE", 1)
    events_dir = tmp_path / "events"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir):
        for i in range(100):
            console.emit({"event": "invoked", "invocation_key": f"k{i}"})
        resolved = writer.events_dir()
        _reset_writer()

        # No assertion on the count - the drain thread races the producer by design.
        # What matters is that emitting far more than the queue holds neither blocks nor raises.
        assert resolved.exists()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is not available on this platform")
def test_a_forked_child_writes_through_its_own_drain_thread(tmp_path):
    """A forked child inherits the parent's writer object and queue but not its drain
    thread, so without the at-fork reset its events would queue forever and never reach
    disk. The child must mint its own writer and actually persist an event."""
    _reset_writer()
    events_dir = tmp_path / "events"
    with writer.CONSOLE_EVENTS_DIR.set_local(events_dir):
        with run_name.RUN_NAME.set_local("2026-08-12/mr.Forked"):
            console.emit({"event": "invoked", "invocation_key": "parent"})

            child = os.fork()
            if child == 0:  # the child: emit, flush, and report via exit code only
                try:
                    console.emit({"event": "invoked", "invocation_key": "child"})
                    assert isinstance(writer._WRITER, writer._Writer)
                    writer._WRITER.close()
                    os._exit(0)
                except BaseException:
                    os._exit(1)

            _, status = os.waitpid(child, 0)
            assert os.waitstatus_to_exitcode(status) == 0

            _reset_writer()  # flush the parent's file too

            by_file = {
                path.name: [json.loads(line)["invocation_key"] for line in path.read_text().splitlines()]
                for path in writer.events_dir().glob("*.jsonl")
            }

            assert by_file[f"events-{child}.jsonl"] == ["child"]
            assert by_file[f"events-{os.getpid()}.jsonl"] == ["parent"]
            # one file per process: the child did not write through the parent's handle.


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is not available on this platform")
def test_forking_while_the_writer_lock_is_held_does_not_deadlock(tmp_path):
    """A lock held by another thread at fork time is copied into the child locked, with
    no thread left to release it. The fork hooks make the fork wait the holder out, and
    the child replaces the locks - without them, the child's first emit hangs forever."""
    import threading
    import time

    _reset_writer()
    with writer.CONSOLE_EVENTS_DIR.set_local(tmp_path / "events"):
        with run_name.RUN_NAME.set_local("2026-08-12/mr.Locked"):
            release = threading.Event()

            def hold():
                with writer._WRITER_LOCK:
                    release.wait(5.0)

            holder = threading.Thread(target=hold)
            holder.start()
            time.sleep(0.05)  # let the holder actually acquire
            threading.Timer(0.3, release.set).start()

            child = os.fork()  # the before-fork hook blocks here until the holder lets go
            if child == 0:
                try:
                    console.emit({"event": "invoked", "invocation_key": "child"})
                    assert isinstance(writer._WRITER, writer._Writer)
                    writer._WRITER.close()
                    os._exit(0)
                except BaseException:
                    os._exit(1)

            deadline = time.monotonic() + 15
            status = None
            while time.monotonic() < deadline:
                pid, status = os.waitpid(child, os.WNOHANG)
                if pid:
                    break

                time.sleep(0.05)
            else:
                os.kill(child, 9)
                os.waitpid(child, 0)
                release.set()
                holder.join()
                raise AssertionError("the forked child hung; its inherited locks were never usable")

            release.set()
            holder.join()
            assert status is not None and os.waitstatus_to_exitcode(status) == 0

            _reset_writer()
            child_events = (writer.events_dir() / f"events-{child}.jsonl").read_text()
            assert json.loads(child_events)["invocation_key"] == "child"
