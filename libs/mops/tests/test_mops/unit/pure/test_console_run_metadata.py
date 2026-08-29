import multiprocessing as mp
import os
import time
from pathlib import Path

import pytest
import tomli

from thds.core import meta
from thds.mops.pure.tools.console import (
    blob_sink,
    run_index,
    run_metadata,
    run_name,
    throwaway,
    upload,
    writer,
)

_MEMO_URI_SUFFIX = "mops2-mpf/pipe/pkg.mod--fn/hash123"


@pytest.fixture(autouse=True)
def _real_run(monkeypatch):
    monkeypatch.setattr(throwaway, "here", lambda: False)
    run_metadata._reset_for_test()
    yield
    run_metadata._reset_for_test()


def _metadata(cwd: Path, command: str = "apps/unified-asset/k8s/run.py --date 2026-08-18"):
    return run_metadata._RunMetadata(
        command=command,
        argv=("apps/unified-asset/k8s/run.py", "--date", "2026-08-18"),
        cwd=str(cwd),
        started_at="2026-08-18T12:34:56+00:00",
        run_name="2026-08-18/mr.Run.abc",
        label="",
        invoked_by="lemon@example",
        hostname="example",
        platform="macOS-15.6-arm64-arm-64bit",
        repo="ds-monorepo",
        branch="mops/a-branch",
        invoker_code_version="20260818.1234-abc1234",
        python_executable="/usr/bin/python3",
        python_version="3.10.18",
        process_id=12345,
    )


def test_filename_describes_the_repo_relative_script(tmp_path):
    assert (
        run_metadata._filename(_metadata(tmp_path))
        == "apps_unified-asset_k8s_run_py--by-lemon_example.toml"
    )


def test_filename_uses_only_the_script_name_outside_the_cwd(tmp_path):
    run = _metadata(tmp_path)._replace(argv=("/usr/local/bin/my-run",))

    assert run_metadata._filename(run) == "my-run--by-lemon_example.toml"


def test_toml_preserves_the_command_and_argv(tmp_path):
    run = _metadata(tmp_path)

    parsed = tomli.loads(run_metadata._to_toml(run))

    assert parsed == run._asdict() | {"argv": list(run.argv)}


def test_the_current_process_is_described_from_its_checkout(monkeypatch):
    monkeypatch.setenv(meta.GIT_BRANCH, "a-branch-from-a-docker-build")

    current = run_metadata._current("2026-08-18/mr.Run.abc")

    assert current.hostname and current.platform
    assert current.repo == "ds-monorepo"
    assert current.branch == "a-branch-from-a-docker-build"
    # the build's record wins over asking git, since an image has no `.git` to ask.
    assert tomli.loads(run_metadata._to_toml(current))["hostname"] == current.hostname


def test_publish_writes_named_metadata_at_the_run_root(tmp_path):
    run = _metadata(tmp_path)
    memo_uri = f"file://{tmp_path}/{_MEMO_URI_SUFFIX}"

    run_metadata._publish(memo_uri, run)

    written = (
        tmp_path
        / "mops/console/2026-08-18/mr.Run.abc/apps_unified-asset_k8s_run_py--by-lemon_example.toml"
    )
    assert tomli.loads(written.read_text()) == run._asdict() | {
        "argv": list(run.argv),
        "label": "lemon@example",
    }
    # nobody labelled the run, so it is published as who started it and where.

    pointer = tmp_path / "mops/console/2026-08-18/_index/123456Z--lemon@example--mr.Run.abc"
    assert pointer.read_text().strip() == f"file://{tmp_path}/mops/console/2026-08-18/mr.Run.abc"


def test_a_root_that_only_served_memoized_results_still_describes_the_run(tmp_path, monkeypatch):
    monkeypatch.setattr(
        run_metadata, "_current", lambda name: _metadata(tmp_path)._replace(run_name=name)
    )
    root = f"file://{tmp_path}/mops/console/2026-08-18/mr.Run.abc"

    assert upload.start_root(root, "2026-08-18/mr.Run.abc")

    assert list((tmp_path / "mops/console/2026-08-18/mr.Run.abc").glob("*.toml"))
    assert list((tmp_path / "mops/console/2026-08-18/_index").iterdir())
    # the history-upload path opens roots that no `invoked` event ever did; without this a
    # console pointed at such a root has no account of the run and no entry in the day's index.


def test_publish_does_not_replace_existing_metadata(tmp_path):
    memo_uri = f"file://{tmp_path}/{_MEMO_URI_SUFFIX}"
    first = _metadata(tmp_path)
    run_metadata._publish(memo_uri, first)

    run_metadata._publish(memo_uri, first._replace(command="a later process"))

    written = (
        tmp_path
        / "mops/console/2026-08-18/mr.Run.abc/apps_unified-asset_k8s_run_py--by-lemon_example.toml"
    )
    assert tomli.loads(written.read_text())["command"] == first.command


def test_a_pointer_is_written_even_when_the_metadata_already_was(tmp_path):
    memo_uri = f"file://{tmp_path}/{_MEMO_URI_SUFFIX}"
    run = _metadata(tmp_path)
    run_metadata._publish(memo_uri, run)
    index = tmp_path / "mops/console/2026-08-18/_index"
    next(index.iterdir()).unlink()
    # the file went out and the pointer did not - a failure between the two writes.

    run_metadata._publish(memo_uri, run)

    assert [path.name for path in index.iterdir()] == ["123456Z--lemon@example--mr.Run.abc"]


def test_a_failed_description_is_retried_on_the_next_opening_of_the_root(tmp_path, monkeypatch):
    monkeypatch.setattr(
        run_metadata, "_current", lambda name: _metadata(tmp_path)._replace(run_name=name)
    )
    root = f"file://{tmp_path}/mops/console/2026-08-18/mr.Run.abc"
    real_publish = run_index.publish
    attempts: list[str] = []

    def flaky(*args, **kwargs):
        attempts.append("pointer")
        if len(attempts) == 1:
            raise OSError("the blob store blinked")

        real_publish(*args, **kwargs)

    monkeypatch.setattr(run_index, "publish", flaky)
    upload._reset()

    assert upload.start_root(root, "2026-08-18/mr.Run.abc")
    assert not list((tmp_path / "mops/console/2026-08-18").glob("_index/*"))

    assert not upload.start_root(root, "2026-08-18/mr.Run.abc")
    # the uploader was already there; the description was not, so it is tried again.
    assert [p.name for p in (tmp_path / "mops/console/2026-08-18/_index").iterdir()] == [
        "123456Z--lemon@example--mr.Run.abc"
    ]

    upload.start_root(root, "2026-08-18/mr.Run.abc")
    assert attempts == ["pointer", "pointer"]
    # and once it is out, later openings leave it alone.


def test_publish_failure_does_not_raise():
    run_metadata.publish("not-a-memo-uri", "2026-08-18/mr.Run.abc")


def test_a_child_process_cannot_publish_run_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv(run_metadata._OWNER_PID_ENV, str(os.getpid() + 1))

    run_metadata.publish(f"file://{tmp_path}/{_MEMO_URI_SUFFIX}", "2026-08-18/mr.Run.abc")

    assert not list(tmp_path.rglob("*.toml"))


def _child_discovers_root(memo_uri: str, run: str) -> None:
    # Attempt publication before telling the parent about the root. If ownership were
    # broken, this would leave child metadata in place before the parent could act.
    throwaway.here = lambda: False
    upload.start(memo_uri, run)
    writer.record_remote_events_uri(blob_sink.events_root(memo_uri, run))


def test_claiming_parent_publishes_a_root_discovered_by_a_child(tmp_path, monkeypatch):
    local_events = tmp_path / "local-events"
    run = "2026-08-18/mr.Run.abc"
    remote_root = tmp_path / "remote"
    memo_uri = f"file://{remote_root}/{_MEMO_URI_SUFFIX}"
    monkeypatch.setenv(writer.CONSOLE_EVENTS_DIR.envname, str(local_events))
    monkeypatch.setenv(run_name.RUN_NAME.envname, run)

    with writer.CONSOLE_EVENTS_DIR.set_local(local_events):
        with run_name.RUN_NAME.set_local(run):
            assert run_name.claim() == run
            child = mp.get_context("spawn").Process(target=_child_discovers_root, args=(memo_uri, run))
            child.start()
            child.join(timeout=10)
            assert child.exitcode == 0

            written = remote_root / "mops/console/2026-08-18/mr.Run.abc"
            deadline = time.monotonic() + 3
            while not list(written.glob("*.toml")) and time.monotonic() < deadline:
                time.sleep(0.05)

    metadata_file = next(iter(written.glob("*.toml")))
    assert tomli.loads(metadata_file.read_text())["process_id"] == os.getpid()
