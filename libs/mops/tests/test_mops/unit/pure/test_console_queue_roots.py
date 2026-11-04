from thds.mops.pure.tools.console import blob_sink, queue_roots, run_name, throwaway

_MEMO_URI = "file://{root}/mops2-mpf/pipe/pkg.mod--fn/hash123"


def test_a_published_queue_root_is_read_back_from_the_runs_events_root(tmp_path, monkeypatch):
    monkeypatch.setattr(throwaway, "here", lambda: False)
    memo_uri = _MEMO_URI.format(root=tmp_path)
    with run_name.RUN_NAME.set_local("2026-10-26/mr.Run.abc"):
        events_root = blob_sink.events_root(memo_uri, run_name.RUN_NAME())
        queue_roots.publish(memo_uri, "file:///elsewhere/queue-b")
        queue_roots.publish(memo_uri, "file:///elsewhere/queue-a")
        queue_roots.publish(memo_uri, "file:///elsewhere/queue-a")

    assert queue_roots.read(events_root) == ["file:///elsewhere/queue-a", "file:///elsewhere/queue-b"]


def test_a_run_with_no_queue_reads_as_empty(tmp_path):
    assert queue_roots.read(f"file://{tmp_path}/mops/console/mr.Run.abc") == []
