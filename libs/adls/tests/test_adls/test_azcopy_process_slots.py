import asyncio
import subprocess
import sys
import threading
import typing as ty
from contextlib import contextmanager
from pathlib import Path

import pytest
from azure.storage.filedatalake import DataLakeFileClient

from thds.adls.azcopy import download, progress, system_resources, upload

_URL = "https://example.blob.core.windows.net/container/file"
_SILENT_CHILD = [sys.executable, "-c", "import time; time.sleep(60)"]
_TALKING_CHILD = [sys.executable, "-c", "print('progress', flush=True); import time; time.sleep(60)"]
_SLOT_COUNT = 2


@pytest.fixture(autouse=True)
def capped_slots(monkeypatch: pytest.MonkeyPatch) -> threading.BoundedSemaphore:
    slots = threading.BoundedSemaphore(_SLOT_COUNT)  # the library has no cap by default
    monkeypatch.setattr(system_resources, "_process_slots", lambda: slots)
    return slots


class _SdkClient:
    url = _URL

    def download_file(self, **_kwargs: ty.Any) -> "_SdkClient":
        return self

    def readinto(self, writer: ty.IO[bytes]) -> None:
        writer.write(b"sdk")


class _AsyncSdkClient:
    url = _URL

    async def download_file(self, **_kwargs: ty.Any) -> "_AsyncSdkClient":
        return self

    async def readinto(self, writer: ty.IO[bytes]) -> None:
        writer.write(b"sdk")


def _never_start_azcopy(*_args: ty.Any, **_kwargs: ty.Any) -> ty.NoReturn:
    raise AssertionError("azcopy started although every process slot was taken")


@contextmanager
def _every_slot_taken() -> ty.Iterator[None]:
    taken = [system_resources.acquire_process_slot() for _ in range(_SLOT_COUNT)]
    try:
        assert all(taken)
        yield
    finally:
        for _ in filter(None, taken):
            system_resources.release_process_slot()


def test_sync_download_uses_sdk_when_every_azcopy_slot_is_taken(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(subprocess, "Popen", _never_start_azcopy)
    with _every_slot_taken():
        download.sync_fastpath(
            ty.cast(DataLakeFileClient, _SdkClient()), download.DownloadRequest(tmp_path / "f", 3)
        )

    assert (tmp_path / "f").read_bytes() == b"sdk"


def test_async_download_uses_sdk_when_every_azcopy_slot_is_taken(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _never_start_azcopy)
    with _every_slot_taken():
        asyncio.run(
            download.async_fastpath(
                ty.cast(DataLakeFileClient, _AsyncSdkClient()),
                download.DownloadRequest(tmp_path / "f", 3),
            )
        )

    assert (tmp_path / "f").read_bytes() == b"sdk"


def test_without_a_cap_every_transfer_may_start_azcopy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(system_resources, "_process_slots", lambda: None)
    assert all(system_resources.acquire_process_slot() for _ in range(_SLOT_COUNT + 1))
    for _ in range(_SLOT_COUNT + 1):
        system_resources.release_process_slot()


def test_failed_azcopy_start_frees_its_slot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _azcopy_missing(*_args: ty.Any, **_kwargs: ty.Any) -> ty.NoReturn:
        raise FileNotFoundError("azcopy")

    monkeypatch.setattr(subprocess, "Popen", _azcopy_missing)
    download.sync_fastpath(
        ty.cast(DataLakeFileClient, _SdkClient()), download.DownloadRequest(tmp_path / "f", 3)
    )

    assert (tmp_path / "f").read_bytes() == b"sdk"
    with _every_slot_taken():
        pass


@contextmanager
def _failing_tracker(*_args: ty.Any) -> ty.Iterator[ty.Callable[[str], None]]:
    def _track(_line: str) -> None:
        raise RuntimeError("progress parsing failed")

    yield _track


def _talking_child_popen(started: list[subprocess.Popen]) -> ty.Callable[..., subprocess.Popen]:
    real_popen = subprocess.Popen

    def _popen(_cmd: ty.Any, **kwargs: ty.Any) -> subprocess.Popen:
        started.append(real_popen(_TALKING_CHILD, **kwargs))
        return started[-1]

    return _popen


def test_failed_sync_download_stops_azcopy_before_freeing_its_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[subprocess.Popen] = []
    monkeypatch.setattr(subprocess, "Popen", _talking_child_popen(started))
    monkeypatch.setattr(progress, "azcopy_tracker", _failing_tracker)
    with pytest.raises(RuntimeError, match="progress parsing failed"):
        download.sync_fastpath(
            ty.cast(DataLakeFileClient, _SdkClient()), download.DownloadRequest(tmp_path / "f", 3)
        )

    assert started[0].poll() is not None
    with _every_slot_taken():
        pass


def test_failed_azcopy_upload_stops_its_process(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[subprocess.Popen] = []
    monkeypatch.setattr(subprocess, "Popen", _talking_child_popen(started))
    monkeypatch.setattr(progress, "azcopy_tracker", _failing_tracker)
    with pytest.raises(RuntimeError, match="progress parsing failed"):
        upload.run(["azcopy"], "adls://example/container/file", 3)

    assert started[0].poll() is not None


def test_cancelled_async_download_stops_azcopy_before_freeing_its_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[asyncio.subprocess.Process] = []
    real_exec = asyncio.create_subprocess_exec

    async def _exec(*_cmd: ty.Any, **kwargs: ty.Any) -> asyncio.subprocess.Process:
        started.append(await real_exec(*_SILENT_CHILD, **kwargs))
        return started[-1]

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)

    async def _cancel_while_azcopy_runs() -> None:
        task = asyncio.create_task(
            download.async_fastpath(
                ty.cast(DataLakeFileClient, _AsyncSdkClient()),
                download.DownloadRequest(tmp_path / "f", 3),
            )
        )
        while not started and not task.done():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert started[0].returncode is not None

    asyncio.run(_cancel_while_azcopy_runs())
    with _every_slot_taken():
        pass


def test_second_cancellation_during_cleanup_still_reaps_azcopy_before_freeing_its_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capped_slots: threading.BoundedSemaphore
) -> None:
    started: list[asyncio.subprocess.Process] = []
    returncodes_at_release: list[int | None] = []
    real_exec = asyncio.create_subprocess_exec

    class _RecordingSlots:
        def acquire(self, blocking: bool) -> bool:
            return capped_slots.acquire(blocking=blocking)

        def release(self) -> None:
            returncodes_at_release.append(started[0].returncode)
            capped_slots.release()

    async def _exec(*_cmd: ty.Any, **kwargs: ty.Any) -> asyncio.subprocess.Process:
        started.append(await real_exec(*_SILENT_CHILD, **kwargs))
        return started[-1]

    monkeypatch.setattr(system_resources, "_process_slots", _RecordingSlots)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)

    async def _cancel_twice() -> None:
        task = asyncio.create_task(
            download.async_fastpath(
                ty.cast(DataLakeFileClient, _AsyncSdkClient()),
                download.DownloadRequest(tmp_path / "f", 3),
            )
        )
        while not started and not task.done():
            await asyncio.sleep(0.01)
        real_kill = started[0].kill

        def _kill_then_cancel_again() -> None:
            real_kill()
            task.cancel()  # lands on the cleanup's wait for the killed process

        monkeypatch.setattr(started[0], "kill", _kill_then_cancel_again)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_cancel_twice())
    assert len(returncodes_at_release) == 1
    assert returncodes_at_release[0] is not None
