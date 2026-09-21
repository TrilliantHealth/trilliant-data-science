"""Run one local cost observer for a Kubernetes target and console run."""

import hashlib
import json
import os
import subprocess
import sys
import threading
import typing as ty
from pathlib import Path

from thds.core import log
from thds.mops.pure.tools.console import run_name, writer

from ..target import K8sTarget
from . import observer, providers

logger = log.getLogger(__name__)

_OWNER_PID_ENV = "THDS_MOPS_CONSOLE_RUN_OWNER_PID"
_ENTRYPOINT = "from thds.mops.k8s.cost.daemon import main; main()"
_STARTED: set[K8sTarget] = set()
_LOCK = threading.Lock()


class _Request(ty.TypedDict):
    kubeconfig_context: str
    namespace: str
    console_run: str
    run_dir: str
    owner_pid: int
    poll_seconds: float
    provider: str


def _owner_pid() -> int:
    try:
        owner_pid = int(os.environ.get(_OWNER_PID_ENV, "0"))
    except ValueError:
        return os.getpid()
    return owner_pid if owner_pid > 0 else os.getpid()


def owner_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _owner_stop(owner_pid: int) -> observer.Stop:
    waiter = threading.Event()

    def stopped(timeout: float) -> bool:
        waiter.wait(timeout)
        return not owner_alive(owner_pid)

    return stopped


def _claim_path(request: _Request) -> Path:
    identity = "\0".join(
        (
            request["console_run"],
            request["kubeconfig_context"],
            request["namespace"],
        )
    )
    key = hashlib.sha256(identity.encode()).hexdigest()[:20]
    return Path(request["run_dir"]) / "costs" / f"observer-{key}.claim"


def _start_process(command: ty.Sequence[str]) -> None:
    subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=True,
    )


def _spawn(request: _Request) -> None:
    _start_process([sys.executable, "-c", _ENTRYPOINT, json.dumps(request, separators=(",", ":"))])


def ensure_started(target: K8sTarget) -> None:
    """Best-effort start of a run-owned observer, including from dispatch workers."""
    try:
        console_run = run_name.claim(bool(writer.CONSOLE_EVENTS_DIR().name))
        if not observer.ENABLED() or not console_run or target in _STARTED:
            return

        with _LOCK:
            if target in _STARTED:
                return
            request = _Request(
                kubeconfig_context=target.kubeconfig_context,
                namespace=target.namespace,
                console_run=console_run,
                run_dir=str(writer.events_dir().resolve()),
                owner_pid=_owner_pid(),
                poll_seconds=observer.POLL_SECONDS(),
                provider=providers.PROVIDER(),
            )
            _spawn(request)
            _STARTED.add(target)
    except Exception:
        logger.debug("Could not start the Kubernetes cost observer; continuing.", exc_info=True)


def _run(request: _Request) -> None:
    claim = _claim_path(request)
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        claim.mkdir()
    except FileExistsError:
        return

    try:
        observer.observe(
            K8sTarget(request["kubeconfig_context"], request["namespace"]),
            request["console_run"],
            Path(request["run_dir"]),
            _owner_stop(request["owner_pid"]),
            request["poll_seconds"],
            request["provider"],
        )
    finally:
        try:
            claim.rmdir()
        except OSError:
            pass


def main(args: ty.Sequence[str] = sys.argv[1:]) -> None:
    if len(args) != 1:
        raise SystemExit("expected one observer request")
    _run(ty.cast(_Request, json.loads(args[0])))
