import concurrent.futures
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

from thds.mops.k8s.cost import daemon
from thds.mops.k8s.target import K8sTarget

_OBSERVER_PROGRAM = """
import os
import sys
import time
from pathlib import Path
from thds.mops.k8s.cost.daemon import owner_alive

owner_pid = int(sys.argv[1])
directory = Path(sys.argv[2])
observations = directory / "observations"
while owner_alive(owner_pid):
    with observations.open("a") as output:
        output.write("observed\\n")
    time.sleep(0.05)
(directory / "stopped").touch()
"""


def _launch_observer_from_dispatch_worker(owner_pid: int, directory: str) -> None:
    daemon._start_process([sys.executable, "-c", _OBSERVER_PROGRAM, str(owner_pid), directory])


def _dispatch_then_wait_for_remote(directory: str) -> None:
    path = Path(directory)
    context = mp.get_context("spawn")
    with concurrent.futures.ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
        pool.submit(_launch_observer_from_dispatch_worker, os.getpid(), directory).result(timeout=10)
    (path / "dispatch-pool-closed").touch()
    while not (path / "remote-complete").exists():
        time.sleep(0.05)


def _wait_for(path: Path, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {path}")
        time.sleep(0.05)


def test_observer_outlives_dispatch_pool_and_stops_with_run_owner(tmp_path):
    context = mp.get_context("spawn")
    owner = context.Process(target=_dispatch_then_wait_for_remote, args=(str(tmp_path),))
    owner.start()
    try:
        _wait_for(tmp_path / "dispatch-pool-closed")
        observations = tmp_path / "observations"
        _wait_for(observations)
        before = len(observations.read_text().splitlines())
        time.sleep(0.2)
        assert len(observations.read_text().splitlines()) > before

        (tmp_path / "remote-complete").touch()
        owner.join(timeout=10)
        assert owner.exitcode == 0
        _wait_for(tmp_path / "stopped")
    finally:
        if owner.is_alive():
            owner.terminate()
            owner.join(timeout=5)


def test_setup_failure_cannot_prevent_job_submission(monkeypatch, tmp_path):
    target = K8sTarget("cluster", "namespace")
    daemon._STARTED.clear()
    monkeypatch.setattr(daemon.run_name, "claim", lambda _: "2026-09-21/mr.Example")
    monkeypatch.setattr(daemon.writer, "events_dir", lambda: tmp_path)
    monkeypatch.setattr(daemon.observer, "ENABLED", lambda: True)

    def fail_to_spawn(_):
        raise OSError("no process")

    monkeypatch.setattr(daemon, "_spawn", fail_to_spawn)

    try:
        daemon.ensure_started(target)
        assert target not in daemon._STARTED
    finally:
        daemon._STARTED.clear()


def test_observer_request_follows_inherited_run_owner(monkeypatch, tmp_path):
    target = K8sTarget("cluster", "namespace")
    requests: list[daemon._Request] = []
    daemon._STARTED.clear()
    monkeypatch.setenv(daemon._OWNER_PID_ENV, "1234")
    monkeypatch.setattr(daemon.run_name, "claim", lambda _: "2026-09-21/mr.Example")
    monkeypatch.setattr(daemon.writer, "events_dir", lambda: tmp_path)
    monkeypatch.setattr(daemon.observer, "ENABLED", lambda: True)
    monkeypatch.setattr(daemon, "_spawn", requests.append)

    try:
        daemon.ensure_started(target)
        assert requests[0]["owner_pid"] == 1234
    finally:
        daemon._STARTED.clear()
