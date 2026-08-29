"""Human-readable identity for an orchestrator run."""

import atexit
import datetime as dt
import json
import os
import platform
import re
import shlex
import sys
import threading
import typing as ty
from pathlib import Path

from thds.core import files, git, hostname, log, meta

from ...core import metadata, uris
from . import blob_sink, run_index

logger = log.getLogger(__name__)

_OWNER_PID_ENV = "THDS_MOPS_CONSOLE_RUN_OWNER_PID"
_DISCOVERY_POLL_SECONDS = 0.25
_DISCOVERY_POLL_AFTER_FIRST_ROOT_SECONDS = 5.0


class _RunMetadata(ty.NamedTuple):
    """Enough to reconstruct how a run was started: the command, where and by whom it was
    run, and which code. Every field is a string except the pid, so the file reads without
    a schema."""

    command: str
    argv: tuple[str, ...]
    cwd: str
    started_at: str
    run_name: str
    label: str  # what the application called the run, else `invoked_by` (`user@host`)
    invoked_by: str
    hostname: str
    platform: str  # OS and machine, e.g. `macOS-15.6-arm64-arm-64bit`
    repo: str  # the git remote's repository name; empty outside a checkout
    branch: str  # empty when detached, or outside a checkout without `GIT_BRANCH` set
    invoker_code_version: str
    python_executable: str
    python_version: str
    process_id: int


def _branch() -> str:
    """The checked-out branch, or the one a docker build recorded for an image with no `.git`."""
    if branch := os.environ.get(meta.GIT_BRANCH):
        return branch

    try:
        return git.get_branch()
    except git.NO_GIT:
        return ""


def _current(run_name: str) -> _RunMetadata:
    argv = tuple(sys.argv)
    return _RunMetadata(
        command=shlex.join(argv),
        argv=argv,
        cwd=str(Path.cwd()),
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        run_name=run_name,
        label="",  # settled when first published, since `label_run` may not have been called yet
        invoked_by=metadata.get_invoked_by(),
        hostname=hostname.friendly(),
        platform=platform.platform(),
        repo=meta.get_repo_name(),
        branch=_branch(),
        invoker_code_version=metadata.get_invoker_code_version(),
        python_executable=sys.executable,
        python_version=platform.python_version(),
        process_id=os.getpid(),
    )


def _script_path(run: _RunMetadata) -> Path:
    if not run.argv or not run.argv[0]:
        return Path("run")

    script = Path(run.argv[0])
    resolved = script.resolve() if script.is_absolute() else (Path(run.cwd) / script).resolve()
    try:
        return resolved.relative_to(Path(run.cwd).resolve())
    except ValueError:
        return Path(script.name)


def _filename(run: _RunMetadata) -> str:
    name = re.sub(r"[^A-Za-z0-9-]+", "_", _script_path(run).as_posix()).strip("_") or "run"
    invoked_by = re.sub(r"[^A-Za-z0-9-]+", "_", run.invoked_by).strip("_") or "unknown"
    return files.shorten_filename(f"{name}--by-{invoked_by}.toml")


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _to_toml(run: _RunMetadata) -> str:
    return "\n".join(
        [
            f"command = {_toml_string(run.command)}",
            f"cwd = {_toml_string(run.cwd)}",
            f"started_at = {_toml_string(run.started_at)}",
            f"run_name = {_toml_string(run.run_name)}",
            f"label = {_toml_string(run.label)}",
            f"invoked_by = {_toml_string(run.invoked_by)}",
            f"hostname = {_toml_string(run.hostname)}",
            f"platform = {_toml_string(run.platform)}",
            f"repo = {_toml_string(run.repo)}",
            f"branch = {_toml_string(run.branch)}",
            f"invoker_code_version = {_toml_string(run.invoker_code_version)}",
            f"python_executable = {_toml_string(run.python_executable)}",
            f"python_version = {_toml_string(run.python_version)}",
            f"process_id = {run.process_id}",
            "argv = [",
            *(f"  {_toml_string(arg)}," for arg in run.argv),
            "]",
            "",
        ]
    )


def _labelled(run: _RunMetadata) -> _RunMetadata:
    return run._replace(label=run_index.freeze_label(run.invoked_by))


def _publish_root(root: str, run: _RunMetadata) -> None:
    run = _labelled(run)
    blob_store = uris.lookup_blob_store(root)
    uri = blob_store.join(root, _filename(run))
    if not blob_store.exists(uri):
        blob_store.putbytes(uri, _to_toml(run).encode(), type_hint="application/toml")

    run_index.publish(root, dt.datetime.fromisoformat(run.started_at), run.label, run.run_name)
    # not behind the metadata check: a pointer is the same bytes every time, so rewriting it
    # is harmless, and a retry after the file went out but the pointer did not still gets one.


def _publish(memo_uri: str, run: _RunMetadata) -> None:
    _publish_root(blob_sink.events_root(memo_uri, run.run_name), run)


_STATE_LOCK = threading.Lock()
_PUBLISH_LOCK = threading.Lock()
_CLAIMED: None | _RunMetadata = None
_PUBLISHED_ROOTS: set[str] = set()
_PUBLISHER: None | threading.Thread = None
_STOP_PUBLISHER = threading.Event()


def _owner_pid() -> int:
    try:
        return int(os.environ.get(_OWNER_PID_ENV, "0"))
    except ValueError:
        return 0


def _claim(run_name: str) -> None | _RunMetadata:
    """Capture identity in, and only in, the process that owns this run."""
    global _CLAIMED
    owner_pid = _owner_pid()
    if owner_pid and owner_pid != os.getpid():
        return None

    with _STATE_LOCK:
        if not _owner_pid():
            os.environ[_OWNER_PID_ENV] = str(os.getpid())
        if _CLAIMED is None:
            _CLAIMED = _current(run_name)
        return _CLAIMED


def _publish_once(root: str, run: _RunMetadata) -> None:
    with _PUBLISH_LOCK:
        if root in _PUBLISHED_ROOTS:
            return
        _publish_root(root, run)
        _PUBLISHED_ROOTS.add(root)


def _publish_discovered_roots(run_dir: Path, run: _RunMetadata) -> bool:
    from . import writer

    found = False
    for root in writer.remote_events_uris(run_dir):
        found = True
        try:
            _publish_once(root, run)
        except Exception:
            logger.debug("Could not publish mops run metadata to %s; continuing.", root, exc_info=True)
    return found


def _watch_for_roots(run_dir: Path, run: _RunMetadata) -> None:
    found_root = False
    while True:
        found_root = _publish_discovered_roots(run_dir, run) or found_root
        delay = _DISCOVERY_POLL_AFTER_FIRST_ROOT_SECONDS if found_root else _DISCOVERY_POLL_SECONDS
        if _STOP_PUBLISHER.wait(delay):
            _publish_discovered_roots(run_dir, run)
            return


def claim(run_name: str) -> None:
    """Make this process the sole publisher for a run claimed before children start."""
    global _PUBLISHER
    run = _claim(run_name)
    if run is None:
        return

    from . import upload, writer

    if (
        not blob_sink.CONSOLE_REMOTE_EVENTS()
        or not upload.CONSOLE_UPLOAD_EVENTS()
        or not writer.CONSOLE_EVENTS_DIR().name
    ):
        return

    with _STATE_LOCK:
        if _PUBLISHER is None:
            run_dir = writer.events_dir().resolve()
            _PUBLISHER = threading.Thread(
                target=_watch_for_roots,
                args=(run_dir, run),
                name="mops-console-run-metadata",
                daemon=True,
            )
            _PUBLISHER.start()
            atexit.register(_stop_publisher)


def publish_root(events_root: str, run_name: str) -> bool:
    """Publish from the run-owning process, never from one of its children.

    True once there is nothing left for this process to do for the root - it is published,
    or describing the run is not this process's job. False means try again later.
    """
    try:
        run = _claim(run_name)
        if run is not None:
            _publish_once(events_root, run)
    except Exception:
        logger.debug("Could not publish mops run metadata; continuing.", exc_info=True)
        return False

    return True


def publish(memo_uri: str, run_name: str) -> None:
    try:
        root = blob_sink.events_root(memo_uri, run_name)
    except Exception:
        logger.debug("Could not publish mops run metadata; continuing.", exc_info=True)
        return

    publish_root(root, run_name)


def _stop_publisher() -> None:
    _STOP_PUBLISHER.set()
    if _PUBLISHER is not None:
        _PUBLISHER.join(timeout=1)


def _reset_after_fork() -> None:
    global _CLAIMED, _PUBLISHER, _STATE_LOCK, _PUBLISH_LOCK, _STOP_PUBLISHER
    _CLAIMED = None
    _PUBLISHER = None
    _STATE_LOCK = threading.Lock()
    _PUBLISH_LOCK = threading.Lock()
    _STOP_PUBLISHER = threading.Event()
    _PUBLISHED_ROOTS.clear()


def _reset_for_test() -> None:
    global _CLAIMED, _PUBLISHER, _STOP_PUBLISHER
    _stop_publisher()
    _CLAIMED = None
    _PUBLISHER = None
    _PUBLISHED_ROOTS.clear()
    _STOP_PUBLISHER = threading.Event()
    os.environ.pop(_OWNER_PID_ENV, None)
    run_index._reset_for_test()


os.register_at_fork(after_in_child=_reset_after_fork)
