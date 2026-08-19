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

from thds.core import files, log

from ...core import metadata, uris
from . import blob_sink

logger = log.getLogger(__name__)

_OWNER_PID_ENV = "THDS_MOPS_CONSOLE_RUN_OWNER_PID"
_DISCOVERY_POLL_SECONDS = 0.25
_DISCOVERY_POLL_AFTER_FIRST_ROOT_SECONDS = 5.0


class _RunMetadata(ty.NamedTuple):
    command: str
    argv: tuple[str, ...]
    cwd: str
    started_at: str
    run_name: str
    invoked_by: str
    invoker_code_version: str
    python_executable: str
    python_version: str
    process_id: int


def _current(run_name: str) -> _RunMetadata:
    argv = tuple(sys.argv)
    return _RunMetadata(
        command=shlex.join(argv),
        argv=argv,
        cwd=str(Path.cwd()),
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        run_name=run_name,
        invoked_by=metadata.get_invoked_by(),
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
            f"invoked_by = {_toml_string(run.invoked_by)}",
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


def _publish_root(root: str, run: _RunMetadata) -> None:
    blob_store = uris.lookup_blob_store(root)
    uri = blob_store.join(root, _filename(run))
    if not blob_store.exists(uri):
        blob_store.putbytes(uri, _to_toml(run).encode(), type_hint="application/toml")


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


def publish(memo_uri: str, run_name: str) -> None:
    """Publish from the run-owning process, never from one of its children."""
    try:
        run = _claim(run_name)
        if run is not None:
            _publish_once(blob_sink.events_root(memo_uri, run.run_name), run)
    except Exception:
        logger.debug("Could not publish mops run metadata; continuing.", exc_info=True)


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


os.register_at_fork(after_in_child=_reset_after_fork)
